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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"log"
	"math"
	"os"
	"path"
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
var specialSoloTypes = map[string]openapi3.Schema{
	"core.solo.io.Metadata": {
		Type: openapi3.TypeObject,
	},
	"google.protobuf.ListValue": *openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema()),
	"google.protobuf.Struct": {
		Type:       openapi3.TypeObject,
		Properties: make(map[string]*openapi3.SchemaRef),
		ExtensionProps: openapi3.ExtensionProps{
			Extensions: map[string]interface{}{
				"x-kubernetes-preserve-unknown-fields": true,
			},
		},
	},
	"google.protobuf.Any": {
		Type:       openapi3.TypeObject,
		Properties: make(map[string]*openapi3.SchemaRef),
		ExtensionProps: openapi3.ExtensionProps{
			Extensions: map[string]interface{}{
				"x-kubernetes-preserve-unknown-fields": true,
			},
		},
	},
	"google.protobuf.Value": {
		Properties: make(map[string]*openapi3.SchemaRef),
		ExtensionProps: openapi3.ExtensionProps{
			Extensions: map[string]interface{}{
				"x-kubernetes-preserve-unknown-fields": true,
			},
		},
	},
	"google.protobuf.BoolValue":   *openapi3.NewBoolSchema().WithNullable(),
	"google.protobuf.StringValue": *openapi3.NewStringSchema().WithNullable(),
	"google.protobuf.DoubleValue": *openapi3.NewFloat64Schema().WithNullable(),
	"google.protobuf.Int32Value":  *openapi3.NewIntegerSchema().WithNullable().WithMin(math.MinInt32).WithMax(math.MaxInt32),
	"google.protobuf.Int64Value":  *openapi3.NewIntegerSchema().WithNullable().WithMin(math.MinInt64).WithMax(math.MaxInt64),
	"google.protobuf.UInt32Value": *openapi3.NewIntegerSchema().WithNullable().WithMin(0).WithMax(math.MaxUint32),
	"google.protobuf.UInt64Value": *openapi3.NewIntegerSchema().WithNullable().WithMin(0).WithMax(math.MaxUint64),
	"google.protobuf.FloatValue":  *openapi3.NewFloat64Schema().WithNullable(),
	"google.protobuf.Duration":    *openapi3.NewStringSchema(),
	"google.protobuf.Empty":       *openapi3.NewObjectSchema().WithMaxProperties(0),
	"google.protobuf.Timestamp":   *openapi3.NewStringSchema().WithFormat("date-time"),
}

type openapiGenerator struct {
	buffer     bytes.Buffer
	model      *protomodel.Model
	perFile    bool
	singleFile bool
	yaml       bool

	// transient state as individual files are processed
	currentPackage             *protomodel.PackageDescriptor
	currentFrontMatterProvider *protomodel.FileDescriptor

	messages map[string]*protomodel.MessageDescriptor

	// @solo.io customizations to limit length of generated descriptions
	descriptionConfiguration *DescriptionConfiguration

	// @solo.io customization to support enum validation schemas with int or string values
	// we need to support this since some controllers marshal enums as integers and others as strings
	enumAsIntOrString bool
}

type DescriptionConfiguration struct {
	// Whether or not to include a description in the generated open api schema
	IncludeDescriptionInSchema bool
}

func newOpenAPIGenerator(
	model *protomodel.Model,
	perFile bool,
	singleFile bool,
	yaml bool,
	descriptionConfiguration *DescriptionConfiguration,
	enumAsIntOrString bool,
) *openapiGenerator {
	return &openapiGenerator{
		model:                    model,
		perFile:                  perFile,
		singleFile:               singleFile,
		yaml:                     yaml,
		descriptionConfiguration: descriptionConfiguration,
		enumAsIntOrString:        enumAsIntOrString,
	}
}

func (g *openapiGenerator) generateOutput(filesToGen map[*protomodel.FileDescriptor]bool) (*plugin.CodeGeneratorResponse, error) {
	response := plugin.CodeGeneratorResponse{}

	if g.singleFile {
		g.generateSingleFileOutput(filesToGen, &response)
	} else {
		for _, pkg := range g.model.Packages {
			g.currentPackage = pkg

			// anything to output for this package?
			count := 0
			for _, file := range pkg.Files {
				if _, ok := filesToGen[file]; ok {
					count++
				}
			}

			if count > 0 {
				if g.perFile {
					g.generatePerFileOutput(filesToGen, pkg, &response)
				} else {
					g.generatePerPackageOutput(filesToGen, pkg, &response)
				}
			}
		}
	}

	return &response, nil
}

func (g *openapiGenerator) getFileContents(file *protomodel.FileDescriptor,
	messages map[string]*protomodel.MessageDescriptor,
	enums map[string]*protomodel.EnumDescriptor,
	services map[string]*protomodel.ServiceDescriptor) {
	for _, m := range file.AllMessages {
		messages[g.relativeName(m)] = m
	}

	for _, e := range file.AllEnums {
		enums[g.relativeName(e)] = e
	}

	for _, s := range file.Services {
		services[g.relativeName(s)] = s
	}
}

func (g *openapiGenerator) generatePerFileOutput(filesToGen map[*protomodel.FileDescriptor]bool, pkg *protomodel.PackageDescriptor,
	response *plugin.CodeGeneratorResponse) {

	for _, file := range pkg.Files {
		if _, ok := filesToGen[file]; ok {
			g.currentFrontMatterProvider = file
			messages := make(map[string]*protomodel.MessageDescriptor)
			enums := make(map[string]*protomodel.EnumDescriptor)
			services := make(map[string]*protomodel.ServiceDescriptor)

			g.getFileContents(file, messages, enums, services)
			filename := path.Base(file.GetName())
			extension := path.Ext(filename)
			name := filename[0 : len(filename)-len(extension)]

			rf := g.generateFile(name, file, messages, enums, services)
			response.File = append(response.File, &rf)
		}
	}

}

func (g *openapiGenerator) generateSingleFileOutput(filesToGen map[*protomodel.FileDescriptor]bool, response *plugin.CodeGeneratorResponse) {
	messages := make(map[string]*protomodel.MessageDescriptor)
	enums := make(map[string]*protomodel.EnumDescriptor)
	services := make(map[string]*protomodel.ServiceDescriptor)

	for file, ok := range filesToGen {
		if ok {
			g.getFileContents(file, messages, enums, services)
		}
	}

	rf := g.generateFile("openapiv3", &protomodel.FileDescriptor{}, messages, enums, services)
	response.File = []*plugin.CodeGeneratorResponse_File{&rf}
}

func (g *openapiGenerator) generatePerPackageOutput(filesToGen map[*protomodel.FileDescriptor]bool, pkg *protomodel.PackageDescriptor,
	response *plugin.CodeGeneratorResponse) {
	// We need to produce a file for this package.

	// Decide which types need to be included in the generated file.
	// This will be all the types in the fileToGen input files, along with any
	// dependent types which are located in packages that don't have
	// a known location on the web.
	messages := make(map[string]*protomodel.MessageDescriptor)
	enums := make(map[string]*protomodel.EnumDescriptor)
	services := make(map[string]*protomodel.ServiceDescriptor)

	g.currentFrontMatterProvider = pkg.FileDesc()

	for _, file := range pkg.Files {
		if _, ok := filesToGen[file]; ok {
			g.getFileContents(file, messages, enums, services)
		}
	}

	rf := g.generateFile(pkg.Name, pkg.FileDesc(), messages, enums, services)
	response.File = append(response.File, &rf)
}

// Generate an OpenAPI spec for a collection of cross-linked files.
func (g *openapiGenerator) generateFile(name string,
	pkg *protomodel.FileDescriptor,
	messages map[string]*protomodel.MessageDescriptor,
	enums map[string]*protomodel.EnumDescriptor,
	_ map[string]*protomodel.ServiceDescriptor) plugin.CodeGeneratorResponse_File {

	g.messages = messages

	allSchemas := make(map[string]*apiext.JSONSchemaProps)

	for _, message := range messages {
		// we generate the top-level messages here and the nested messages are generated
		// inside each top-level message.
		if message.Parent == nil {
			g.generateMessage(message, allSchemas)
		}
	}

	for _, enum := range enums {
		// when there is no parent to the enum.
		if len(enum.QualifiedName()) == 1 {
			g.generateEnum(enum, allSchemas)
		}
	}

	// only get the API version when generate per package or per file,
	// as we cannot guarantee all protos in the input are the same version.
	//if !g.singleFile {
	//	if g.currentFrontMatterProvider != nil && g.currentFrontMatterProvider.Matter.Description != "" {
	//		description = g.currentFrontMatterProvider.Matter.Description
	//	} else if pd := g.generateDescription(g.currentPackage); pd != "" {
	//		description = pd
	//	} else {
	//		description = "OpenAPI Spec for Solo APIs."
	//	}
	//	// derive the API version from the package name
	//	// which is a convention for Istio APIs.
	//	var p string
	//	if pkg != nil {
	//		p = pkg.GetPackage()
	//	} else {
	//		p = name
	//	}
	//	s := strings.Split(p, ".")
	//	version = s[len(s)-1]
	//} else {
	//	description = "OpenAPI Spec for Solo APIs."
	//}

	// add the openapi object required by the spec.

	groupKind := schema.GroupKind{
		Group: "group",
		Kind:  "kind",
	}

	crd := apiext.CustomResourceDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apiextensions.k8s.io/v1",
			Kind:       "CustomResourceDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "plural" + "." + groupKind.Group,
		},
		Spec: apiext.CustomResourceDefinitionSpec{
			Group: groupKind.Group,
			Names: apiext.CustomResourceDefinitionNames{
				Kind:     groupKind.Kind,
				ListKind: groupKind.Kind + "List",
				Plural:   "plural",
				Singular: strings.ToLower(groupKind.Kind),
			},
			Scope: apiext.NamespaceScoped,
		},
	}
	ver := apiext.CustomResourceDefinitionVersion{
		Name:   "version",
		Served: true,
		Schema: &apiext.CustomResourceValidation{
			OpenAPIV3Schema: allSchemas["istio.extensions.v1alpha1.WasmPlugin"],
		},
	}
	crd.Spec.Versions = append(crd.Spec.Versions, ver)

	g.buffer.Reset()
	var filename *string
	if g.yaml {
		b, err := yaml.Marshal(crd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to marshall the output of %v to yaml", name)
		}
		filename = proto.String(name + ".yaml")
		g.buffer.Write(b)
	} else {
		b, err := json.MarshalIndent(crd, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to marshall the output of %v to json", name)
		}
		filename = proto.String(name + ".json")
		g.buffer.Write(b)
	}

	return plugin.CodeGeneratorResponse_File{
		Name:    filename,
		Content: proto.String(g.buffer.String()),
	}
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

func (g *openapiGenerator) generateMessageSchema(message *protomodel.MessageDescriptor) *apiext.JSONSchemaProps {
	// skip MapEntry message because we handle map using the map's repeated field.
	if message.GetOptions().GetMapEntry() {
		return nil
	}
	log.Println(message.String())
	o := &apiext.JSONSchemaProps{
		Type:       "object",
		Properties: make(map[string]apiext.JSONSchemaProps),
	}
	o.Description = g.generateDescription(message)

	for _, field := range message.Fields {
		sr := g.fieldType(field)
		o.Properties[g.fieldName(field)] = *sr
	}

	return o
}

func (g *openapiGenerator) generateEnum(enum *protomodel.EnumDescriptor, allSchemas map[string]*apiext.JSONSchemaProps) {
	o := g.generateEnumSchema(enum)
	allSchemas[g.absoluteName(enum)] = o
}

func (g *openapiGenerator) generateEnumSchema(enum *protomodel.EnumDescriptor) *apiext.JSONSchemaProps {
	o := &apiext.JSONSchemaProps{Type: "string"}
	o.Description = g.generateDescription(enum)

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
	t := strings.Split(c, "\n\n")[0]
	// omit the comment that starts with `$`.
	if strings.HasPrefix(t, "$") {
		return ""
	}

	return strings.Join(strings.Fields(t), " ")
}

func (g *openapiGenerator) fieldType(field *protomodel.FieldDescriptor) *apiext.JSONSchemaProps {
	schema := &apiext.JSONSchemaProps{}
	var isMap bool
	switch *field.Type {
	case descriptor.FieldDescriptorProto_TYPE_FLOAT, descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		schema.Type = "number"

	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_SINT32, descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		schema.Type = "number"
		schema.Format = "int32"

	case descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_SINT64, descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		schema = g.generateSoloInt64Schema()

	case descriptor.FieldDescriptorProto_TYPE_UINT64, descriptor.FieldDescriptorProto_TYPE_FIXED64:
		schema = g.generateSoloInt64Schema()

	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_FIXED32:
		schema.Type = "number"
		schema.Format = "int32"

	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		schema.Type = "boolean"

	case descriptor.FieldDescriptorProto_TYPE_STRING:
		schema.Type = "string"

	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		msg := field.FieldType.(*protomodel.MessageDescriptor)
		if msg.GetOptions().GetMapEntry() {
			isMap = true
			sr := g.fieldType(msg.Fields[1])
			//schema.Type = "object"
			schema = sr

		} else {
			schema = g.generateMessageSchema(msg)
		}

	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		schema.Type = "string"
		schema.Format = "byte"

	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		enum := field.FieldType.(*protomodel.EnumDescriptor)
		schema = g.generateEnumSchema(enum)
	}

	if field.IsRepeated() && !isMap {
		schema.Format = "array"
		// TODO?
		schema.Items = &apiext.JSONSchemaPropsOrArray{Schema: schema}
	}

	if schema != nil {
		schema.Description = g.generateDescription(field)
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
