// Copyright 2019 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this currentFile except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"slices"
	"sort"
	"strings"

	"github.com/solo-io/protoc-gen-openapi/pkg/protomodel"
	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

// Some special types with predefined schemas.
// This is to catch cases where solo apis contain recursive definitions
// Normally these would result in stack-overflow errors when generating the open api schema
// The imperfect solution, is to just generate an empty object for these types
var specialSoloTypes = map[string]*apiext.JSONSchemaProps{
	"core.solo.io.Metadata": {
		Type: openapi3.TypeObject,
	},
	"google.protobuf.ListValue": {
		Items: &apiext.JSONSchemaPropsOrArray{Schema: &apiext.JSONSchemaProps{Type: "object"}},
	},
	"google.protobuf.Struct": {
		Type:                   openapi3.TypeObject,
		XPreserveUnknownFields: Ptr(true),
	},
	"google.protobuf.Any": {
		Type:                   openapi3.TypeObject,
		XPreserveUnknownFields: Ptr(true),
	},
	"google.protobuf.Value": {
		XPreserveUnknownFields: Ptr(true),
	},
	"google.protobuf.BoolValue": {
		Type:     "boolean",
		Nullable: true,
	},
	"google.protobuf.StringValue": {
		Type:     "string",
		Nullable: true,
	},
	"google.protobuf.DoubleValue": {
		Type:     "number",
		Nullable: true,
	},
	"google.protobuf.Int32Value": {
		Type:     "integer",
		Nullable: true,
		//Min: math.MinInt32,
		//Max: math.MaxInt32,
	},
	"google.protobuf.Int64Value": {
		Type:     "integer",
		Nullable: true,
		//Min: math.MinInt64,
		//Max: math.MaxInt64,
	},
	"google.protobuf.UInt32Value": {
		Type:     "integer",
		Nullable: true,
		//Min: 0,
		//Max: math.MaxUInt32,
	},
	"google.protobuf.UInt64Value": {
		Type:     "integer",
		Nullable: true,
		//Min: 0,
		//Max: math.MaxUInt62,
	},
	"google.protobuf.FloatValue": {
		Type:     "number",
		Nullable: true,
	},
	"google.protobuf.Duration": {
		Type:     "string",
		Nullable: true,
	},
	"google.protobuf.Empty": {
		Type:          "object",
		MaxProperties: Ptr(int64(0)),
	},
	"google.protobuf.Timestamp": {
		Type:   "string",
		Format: "date-time",
	},
}

type openapiGenerator struct {
	model *protomodel.Model

	// transient state as individual files are processed
	currentPackage             *protomodel.PackageDescriptor
	currentFrontMatterProvider *protomodel.FileDescriptor

	messages map[string]*protomodel.MessageDescriptor

	// @solo.io customizations to limit length of generated descriptions
	descriptionConfiguration *DescriptionConfiguration

	// @solo.io customization to support enum validation schemas with int or string values
	// we need to support this since some controllers marshal enums as integers and others as strings
	enumAsIntOrString bool

	// @solo.io customizations to define schemas for certain messages
	customSchemasByMessageName map[string]*apiext.JSONSchemaProps
}

type DescriptionConfiguration struct {
	// Whether or not to include a description in the generated open api schema
	IncludeDescriptionInSchema bool
}

func newOpenAPIGenerator(
	model *protomodel.Model,
	descriptionConfiguration *DescriptionConfiguration,
	enumAsIntOrString bool,
) *openapiGenerator {
	return &openapiGenerator{
		model:                      model,
		descriptionConfiguration:   descriptionConfiguration,
		enumAsIntOrString:          enumAsIntOrString,
		customSchemasByMessageName: buildCustomSchemasByMessageName(),
	}
}

// buildCustomSchemasByMessageName name returns a mapping of message name to a pre-defined openapi schema
// It includes:
//  1. `specialSoloTypes`, a set of pre-defined schemas
func buildCustomSchemasByMessageName() map[string]*apiext.JSONSchemaProps {
	schemasByMessageName := make(map[string]*apiext.JSONSchemaProps)

	// Initialize the hard-coded values
	for name, schema := range specialSoloTypes {
		schemasByMessageName[name] = schema
	}

	return schemasByMessageName
}

func (g *openapiGenerator) generateOutput(filesToGen map[*protomodel.FileDescriptor]bool) (*plugin.CodeGeneratorResponse, error) {
	response := plugin.CodeGeneratorResponse{}

	g.generateSingleFileOutput(filesToGen, &response)

	return &response, nil
}

func (g *openapiGenerator) getFileContents(
	file *protomodel.FileDescriptor,
	messages map[string]*protomodel.MessageDescriptor,
	enums map[string]*protomodel.EnumDescriptor,
	descriptions map[string]string,
) {
	for _, m := range file.AllMessages {
		messages[g.relativeName(m)] = m
	}

	for _, e := range file.AllEnums {
		enums[g.relativeName(e)] = e
	}
	for _, v := range file.Matter.Extra {
		if _, n, f := strings.Cut(v, "schema: "); f {
			descriptions[n] = fmt.Sprintf("%v See more details at: %v", file.Matter.Description, file.Matter.HomeLocation)
		}
	}
}

func (g *openapiGenerator) generateSingleFileOutput(filesToGen map[*protomodel.FileDescriptor]bool, response *plugin.CodeGeneratorResponse) {
	messages := make(map[string]*protomodel.MessageDescriptor)
	enums := make(map[string]*protomodel.EnumDescriptor)
	descriptions := make(map[string]string)

	for file, ok := range filesToGen {
		if ok {
			g.getFileContents(file, messages, enums, descriptions)
		}
	}

	rf := g.generateFile("openapiv3", messages, enums, descriptions)
	response.File = []*plugin.CodeGeneratorResponse_File{&rf}
}

const (
	enableCRDGenTag = "+cue-gen"
)

func cleanComments(lines []string) []string {
	out := []string{}
	var prevLine string
	for _, line := range lines {
		line = strings.Trim(line, " ")

		if line == "-->" {
			out = append(out, prevLine)
			prevLine = ""
			continue
		}

		if !strings.HasPrefix(line, enableCRDGenTag) {
			if prevLine != "" && len(line) != 0 {
				prevLine += " " + line
			}
			continue
		}

		out = append(out, prevLine)

		prevLine = line

	}
	if prevLine != "" {
		out = append(out, prevLine)
	}
	return out
}
func parseGenTags(s string) map[string]string {
	lines := cleanComments(strings.Split(s, "\n"))
	res := map[string]string{}
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		_, contents, f := strings.Cut(line, enableCRDGenTag)
		if !f {
			continue
		}
		//res[contents] = ""
		//continue
		spl := strings.SplitN(contents[1:], ":", 3)
		if len(spl) < 2 {
			log.Fatalf("invalid tag: %v", line)
		}
		val := ""
		if len(spl) > 2 {
			val = spl[2]
		}
		res[spl[1]] = val
	}
	if len(res) == 0 {
		return nil
	}
	return res
}

// Generate an OpenAPI spec for a collection of cross-linked files.
func (g *openapiGenerator) generateFile(name string, messages map[string]*protomodel.MessageDescriptor, enums map[string]*protomodel.EnumDescriptor, descriptions map[string]string) plugin.CodeGeneratorResponse_File {

	g.messages = messages

	allSchemas := make(map[string]*apiext.JSONSchemaProps)

	// Type --> Key --> Value
	genTags := map[string]map[string]string{}

	for _, message := range messages {
		// we generate the top-level messages here and the nested messages are generated
		// inside each top-level message.
		if message.Parent == nil {
			g.generateMessage(message, allSchemas)
		}
		if gt := parseGenTags(message.Location().GetLeadingComments()); gt != nil {
			genTags[g.absoluteName(message)] = gt
		}
	}
	for k, kv := range genTags {
		for kk, vv := range kv {
			log.Printf("%q %q %q", k, kk, vv)
		}
	}

	for _, enum := range enums {
		// when there is no parent to the enum.
		if len(enum.QualifiedName()) == 1 {
			g.generateEnum(enum, allSchemas)
		}
	}

	crds := []*apiext.CustomResourceDefinition{}

	for name, cfg := range genTags {
		log.Println("Generating", name)
		group := cfg["groupName"]
		version := cfg["version"]
		kind := name[strings.LastIndex(name, ".")+1:]
		singular := strings.ToLower(kind)
		plural := singular + "s"
		spec := *allSchemas[name]
		if d, f := descriptions[name]; f {
			spec.Description = d
		}
		schema := &apiext.JSONSchemaProps{
			Type: "object",
			Properties: map[string]apiext.JSONSchemaProps{
				"spec": spec,
				"status": {
					Type:                   "object",
					XPreserveUnknownFields: Ptr(true),
				},
			},
		}
		crd := &apiext.CustomResourceDefinition{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "apiextensions.k8s.io/v1",
				Kind:       "CustomResourceDefinition",
			},
			ObjectMeta: metav1.ObjectMeta{
				Annotations: extractKV(cfg["annotations"]),
				Labels:      extractKV(cfg["labels"]),
			},
			Spec: apiext.CustomResourceDefinitionSpec{
				Group: group,
				Names: apiext.CustomResourceDefinitionNames{
					Kind:     kind,
					ListKind: kind + "List",
					Plural:   plural,
					Singular: singular,
				},
				Scope: apiext.NamespaceScoped,
			},
			Status: apiext.CustomResourceDefinitionStatus{},
		}
		ver := apiext.CustomResourceDefinitionVersion{
			Name:   version,
			Served: true,
			Schema: &apiext.CustomResourceValidation{
				OpenAPIV3Schema: schema,
			},
		}

		if res, f := cfg["resource"]; f {
			for n, m := range extractKV(res) {
				log.Println(n, m)
				switch n {
				case "categories":
					crd.Spec.Names.Categories = mergeSlices(crd.Spec.Names.Categories, strings.Split(m, ","))
				case "plural":
					crd.Spec.Names.Plural = m
				case "kind":
					crd.Spec.Names.Kind = m
				case "shortNames":
					crd.Spec.Names.ShortNames = mergeSlices(crd.Spec.Names.ShortNames, strings.Split(m, ","))
				case "singular":
					crd.Spec.Names.Singular = m
				case "listKind":
					crd.Spec.Names.ListKind = m
				}
			}
		}
		crd.Name = crd.Spec.Names.Plural + "." + group
		if pk, f := cfg["printerColumn"]; f {
			pcs := strings.Split(pk, ";;")
			for _, pc := range pcs {
				if pc == "" {
					continue
				}
				column := apiext.CustomResourceColumnDefinition{}
				for n, m := range extractKeyValue(pc) {
					switch n {
					case "name":
						column.Name = m
					case "type":
						column.Type = m
					case "description":
						column.Description = m
					case "JSONPath":
						column.JSONPath = m
					}
				}
				ver.AdditionalPrinterColumns = append(ver.AdditionalPrinterColumns, column)
			}
		}
		if sr, f := cfg["subresource"]; f {
			if sr == "status" {
				ver.Subresources = &apiext.CustomResourceSubresources{Status: &apiext.CustomResourceSubresourceStatus{}}
			}
		}
		if _, f := cfg["storageVersion"]; f {
			ver.Storage = true
		}
		crd.Spec.Versions = append(crd.Spec.Versions, ver)
		crds = append(crds, crd)
	}

	slices.SortFunc(crds, func(a, b *apiext.CustomResourceDefinition) int {
		if a.Name < b.Name {
			return 1
		} else {
			return -1
		}
	})
	bb := &bytes.Buffer{}
	bb.WriteString("# DO NOT EDIT - Generated by Cue OpenAPI generator based on Istio APIs.\n")
	for i, crd := range crds {
		b, err := yaml.Marshal(crd)
		if err != nil {
			log.Fatalf("unable to marshall the output of %v to yaml", name)
		}
		b = fixupYaml(b)
		bb.Write(b)
		if i != len(crds)-1 {
			bb.WriteString("---\n")
		}
	}

	return plugin.CodeGeneratorResponse_File{
		Name:    proto.String(name + ".yaml"),
		Content: proto.String(bb.String()),
	}
}

func mergeSlices(a []string, b []string) []string {
	nv := sets.New(a...).Insert(b...).UnsortedList()
	sort.Strings(nv)
	return nv
}

func extractKV(s string) map[string]string {
	return extractKeyValue(s)
	res := map[string]string{}
	for _, i := range strings.Split(s, ",") {
		k, v, _ := strings.Cut(i, "=")
		res[k] = v
	}
	return res
}

// extractkeyValue extracts a string to key value pairs
// e.g. a=b,b=c to map[a:b b:c]
// and a=b,c,d,e=f to map[a:b,c,d e:f]
func extractKeyValue(s string) map[string]string {
	out := map[string]string{}
	if s == "" {
		return out
	}
	splits := strings.Split(s, "=")
	if len(splits) == 1 {
		out[splits[0]] = ""
	}
	if strings.Contains(splits[0], ",") {
		log.Fatalf("cannot parse %v to key value pairs", s)
	}
	nextkey := splits[0]
	for i := 1; i < len(splits); i++ {
		if splits[i] == "" || splits[i] == "," {
			log.Fatalf("cannot parse %v to key value paris, invalid value", s)
		}
		if !strings.Contains(splits[i], ",") && i != len(splits)-1 {
			log.Fatalf("cannot parse %v to key value pairs, missing separator", s)
		}
		if i == len(splits)-1 {
			out[nextkey] = strings.Trim(splits[i], "\"'`")
			continue
		}
		index := strings.LastIndex(splits[i], ",")
		out[nextkey] = strings.Trim(splits[i][:index], "\"'`")
		nextkey = splits[i][index+1:]
		if nextkey == "" {
			log.Fatalf("cannot parse %v to key value pairs, missing key", s)
		}
	}
	return out
}

const (
	statusOutput = `
status:
  acceptedNames:
    kind: ""
    plural: ""
  conditions: null
  storedVersions: null`

	creationTimestampOutput = `
  creationTimestamp: null`
)

func fixupYaml(y []byte) []byte {
	// remove the status and creationTimestamp fields from the output. Ideally we could use OrderedMap to remove those.
	y = bytes.ReplaceAll(y, []byte(statusOutput), []byte(""))
	y = bytes.ReplaceAll(y, []byte(creationTimestampOutput), []byte(""))
	// keep the quotes in the output which is required by helm.
	y = bytes.ReplaceAll(y, []byte("helm.sh/resource-policy: keep"), []byte(`"helm.sh/resource-policy": keep`))
	return y
}

func (g *openapiGenerator) generateMessage(message *protomodel.MessageDescriptor, allSchemas map[string]*apiext.JSONSchemaProps) {
	if o := g.generateMessageSchema(message); o != nil {
		allSchemas[g.absoluteName(message)] = o
	}
}

func (g *openapiGenerator) generateSoloInt64Schema() *apiext.JSONSchemaProps {
	return &apiext.JSONSchemaProps{
		Type:         "integer",
		Format:       "int64",
		XIntOrString: true,
	}
}

func (g *openapiGenerator) generateCustomMessageSchema(message *protomodel.MessageDescriptor, customSchema *apiext.JSONSchemaProps) *apiext.JSONSchemaProps {
	o := customSchema
	o.Description = g.generateDescription(message)

	return o
}

func (g *openapiGenerator) generateMessageSchema(message *protomodel.MessageDescriptor) *apiext.JSONSchemaProps {
	// skip MapEntry message because we handle map using the map's repeated field.
	if message.GetOptions().GetMapEntry() {
		return nil
	}
	o := &apiext.JSONSchemaProps{
		Type:       "object",
		Properties: make(map[string]apiext.JSONSchemaProps),
	}
	o.Description = g.generateDescription(message)

	oneOfs := make([]apiext.JSONSchemaProps, len(message.OneofDecl))
	for _, field := range message.Fields {
		fn := g.fieldName(field)
		if field.OneofIndex != nil {
			oneOfs[*field.OneofIndex].OneOf = append(oneOfs[*field.OneofIndex].OneOf, apiext.JSONSchemaProps{Required: []string{fn}})
		}
		sr := g.fieldType(field)
		o.Properties[fn] = *sr
	}
	for i, oo := range oneOfs {
		oo.OneOf = append([]apiext.JSONSchemaProps{{Not: &apiext.JSONSchemaProps{AnyOf: oo.OneOf}}}, oo.OneOf...)
		oneOfs[i] = oo
	}
	switch len(oneOfs) {
	case 0:
	case 1:
		o.OneOf = oneOfs[0].OneOf
	default:
		o.AllOf = oneOfs
	}

	return o
}

func (g *openapiGenerator) generateEnum(enum *protomodel.EnumDescriptor, allSchemas map[string]*apiext.JSONSchemaProps) {
	o := g.generateEnumSchema(enum)
	allSchemas[g.absoluteName(enum)] = o
}

func (g *openapiGenerator) generateEnumSchema(enum *protomodel.EnumDescriptor) *apiext.JSONSchemaProps {
	o := &apiext.JSONSchemaProps{Type: "string"}
	// Enum description is not used in Kubernetes
	//o.Description = g.generateDescription(enum)

	// If the schema should be int or string, mark it as such
	if g.enumAsIntOrString {
		o.XIntOrString = true
		return o
	}

	// otherwise, return define the expected string values
	values := enum.GetValue()
	for _, v := range values {
		b, _ := json.Marshal(v.GetName())
		o.Enum = append(o.Enum, apiext.JSON{Raw: b})
	}
	o.Type = "string"

	return o
}

func (g *openapiGenerator) absoluteName(desc protomodel.CoreDesc) string {
	typeName := protomodel.DottedName(desc)
	return desc.PackageDesc().Name + "." + typeName
}

// converts the first section of the leading comment or the description of the proto
// to a single line of description.
func (g *openapiGenerator) generateDescription(desc protomodel.CoreDesc) string {
	if !g.descriptionConfiguration.IncludeDescriptionInSchema {
		return ""
	}

	c := strings.TrimSpace(desc.Location().GetLeadingComments())
	if strings.Contains(c, "$hide_from_docs") {
		return ""
	}
	t := strings.Split(c, "\n\n")[0]
	//return strings.Split(t, "\n")[0]
	//// omit the comment that starts with `$`.
	//if strings.HasPrefix(t, "$") {
	//	return ""
	//}

	idx := strings.Index(t, ".")
	if idx == -1 {
		return t
	}
	return t[:idx+1]
	return strings.Join(strings.Fields(t), " ")
}

func (g *openapiGenerator) fieldType(field *protomodel.FieldDescriptor) *apiext.JSONSchemaProps {
	schema := &apiext.JSONSchemaProps{}
	var isMap bool
	switch *field.Type {
	case descriptor.FieldDescriptorProto_TYPE_FLOAT, descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		schema.Type = "number"
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		schema.Type = "integer"
		//schema.Format = "int32"
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		schema = g.generateSoloInt64Schema()
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		schema = g.generateSoloInt64Schema()
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		schema.Type = "integer"
		//schema.Format = "int32"
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		schema.Type = "boolean"
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_STRING:
		schema.Type = "string"
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		msg := field.FieldType.(*protomodel.MessageDescriptor)
		if customSchema, ok := g.customSchemasByMessageName[g.absoluteName(msg)]; ok {
			schema = g.generateCustomMessageSchema(msg, customSchema)
		} else if msg.GetOptions().GetMapEntry() {
			isMap = true
			sr := g.fieldType(msg.Fields[1])
			//schema.Type = "object"
			schema = sr
			schema = &apiext.JSONSchemaProps{
				//Format: "array",
				Type:                 "object",
				AdditionalProperties: &apiext.JSONSchemaPropsOrBool{Schema: schema},
			}

		} else {
			schema = g.generateMessageSchema(msg)
		}
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		schema.Type = "string"
		schema.Format = "byte"
		schema.Description = g.generateDescription(field)

	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		enum := field.FieldType.(*protomodel.EnumDescriptor)
		schema = g.generateEnumSchema(enum)
		schema.Description = g.generateDescription(field)
	}

	if field.IsRepeated() && !isMap {
		schema = &apiext.JSONSchemaProps{
			//Format: "array",
			Type:  "array",
			Items: &apiext.JSONSchemaPropsOrArray{Schema: schema},
		}
		schema.Description = schema.Items.Schema.Description
		schema.Items.Schema.Description = ""
	}

	return schema
}

func (g *openapiGenerator) fieldName(field *protomodel.FieldDescriptor) string {
	return field.GetJsonName()
}

func (g *openapiGenerator) relativeName(desc protomodel.CoreDesc) string {
	typeName := protomodel.DottedName(desc)
	if desc.PackageDesc() == g.currentPackage {
		return typeName
	}

	return desc.PackageDesc().Name + "." + typeName
}

func Ptr[T any](t T) *T {
	return &t
}
