package validation

import (
	"embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xeipuuv/gojsonschema"

	"github.com/notification-system-moxicom/orchestrator/internal/errors"
)

//go:embed schemas/*.json
var schemaFS embed.FS

type JSONSchemaMessageValidator struct {
	schemaMap map[string]gojsonschema.JSONLoader
}

func NewJSONSchemaMessageValidator(
	schemaFiles map[string]string,
) (*JSONSchemaMessageValidator, error) {
	validator := &JSONSchemaMessageValidator{
		schemaMap: make(map[string]gojsonschema.JSONLoader),
	}

	for messageType, schemaFile := range schemaFiles {
		schemaContent, err := schemaFS.ReadFile(schemaFile)
		if err != nil {
			return nil, errors.NewInvalidMessageError(
				"failed to read schema file: "+err.Error(),
				err,
			)
		}

		schemaLoader := gojsonschema.NewStringLoader(string(schemaContent))
		validator.schemaMap[messageType] = schemaLoader
	}

	return validator, nil
}

func (v *JSONSchemaMessageValidator) Validate(message any) error {
	messageType := getTypeName(message)

	schema, exists := v.schemaMap[messageType]
	if !exists {
		return errors.NewInvalidMessageError("No JSON schema found for "+messageType, nil)
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return errors.NewInvalidMessageError(
			"Failed to marshal message to JSON for "+messageType,
			err,
		)
	}

	var jsonData any

	if err := json.Unmarshal(messageBytes, &jsonData); err != nil {
		return errors.NewInvalidMessageError(
			"Failed to unmarshal message JSON for "+messageType,
			err,
		)
	}

	result, err := gojsonschema.Validate(schema, gojsonschema.NewGoLoader(jsonData))
	if err != nil {
		return errors.NewInvalidMessageError("Failed to validate JSON for "+messageType, err)
	}

	if !result.Valid() {
		var errs []string

		for _, desc := range result.Errors() {
			errs = append(errs, desc.String())
		}

		return errors.NewInvalidMessageError(
			"JSON validation failed for "+messageType+": "+strings.Join(errs, ", "),
			nil,
		)
	}

	return nil
}

func getTypeName(msg any) string {
	if msg == nil {
		return ""
	}

	return strings.TrimPrefix(strings.TrimPrefix(fmt.Sprintf("%T", msg), "*"), "message.")
}
