package validator

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	namePattern     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_\-]{2,63}$`)
	slugPattern     = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
	uuidPattern     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	urlPattern      = regexp.MustCompile(`^https?://[^\s/$.?#].[^\s]*$`)
	cronPattern     = regexp.MustCompile(`^(@(annually|yearly|monthly|weekly|daily|hourly|every_\d+[smhd]))|(((?:[0-5]?\d|\*)\s){4}(?:[0-5]?\d|\*))$`)
	positiveInteger = regexp.MustCompile(`^[1-9]\d*$`)
)

type Rule func(value any) error

type ValidationRules map[string][]Rule

type Validator struct {
	rules    ValidationRules
	optional bool
}

func New() *Validator {
	return &Validator{
		rules: make(ValidationRules),
	}
}

func (v *Validator) Add(field string, rules ...Rule) *Validator {
	v.rules[field] = append(v.rules[field], rules...)
	return v
}

func (v *Validator) Optional() *Validator {
	v.optional = true
	return v
}

func (v *Validator) Validate(data map[string]any) error {
	if data == nil && v.optional {
		return nil
	}

	var errs []string
	for field, rules := range v.rules {
		val, exists := data[field]
		if !exists {
			if !v.optional {
				errs = append(errs, fmt.Sprintf("%s: required field", field))
			}
			continue
		}

		for _, rule := range rules {
			if err := rule(val); err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", field, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

func Required() Rule {
	return func(value any) error {
		if value == nil {
			return fmt.Errorf("is required")
		}
		switch v := value.(type) {
		case string:
			if strings.TrimSpace(v) == "" {
				return fmt.Errorf("must not be empty")
			}
		}
		return nil
	}
}

func Min(n int) Rule {
	return func(value any) error {
		switch v := value.(type) {
		case int:
			if v < n {
				return fmt.Errorf("must be at least %d", n)
			}
		case float64:
			if int(v) < n {
				return fmt.Errorf("must be at least %d", n)
			}
		case string:
			if len(v) < n {
				return fmt.Errorf("length must be at least %d", n)
			}
		}
		return nil
	}
}

func Max(n int) Rule {
	return func(value any) error {
		switch v := value.(type) {
		case int:
			if v > n {
				return fmt.Errorf("must be at most %d", n)
			}
		case float64:
			if int(v) > n {
				return fmt.Errorf("must be at most %d", n)
			}
		case string:
			if len(v) > n {
				return fmt.Errorf("length must be at most %d", n)
			}
		}
		return nil
	}
}

func InRange(min, max int) Rule {
	return func(value any) error {
		switch v := value.(type) {
		case int:
			if v < min || v > max {
				return fmt.Errorf("must be between %d and %d", min, max)
			}
		case float64:
			iv := int(v)
			if iv < min || iv > max {
				return fmt.Errorf("must be between %d and %d", min, max)
			}
		}
		return nil
	}
}

func ValidName() Rule {
	return func(value any) error {
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a string")
		}
		if !namePattern.MatchString(s) {
			return fmt.Errorf("must match pattern: alphanumeric, hyphens, underscores, 3-64 chars")
		}
		return nil
	}
}

func ValidSlug() Rule {
	return func(value any) error {
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a string")
		}
		if !slugPattern.MatchString(s) {
			return fmt.Errorf("must be a valid slug (lowercase, hyphens)")
		}
		return nil
	}
}

func ValidUUID() Rule {
	return func(value any) error {
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a string")
		}
		if !uuidPattern.MatchString(s) {
			return fmt.Errorf("must be a valid UUID")
		}
		return nil
	}
}

func ValidURL() Rule {
	return func(value any) error {
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a string")
		}
		if !urlPattern.MatchString(s) {
			return fmt.Errorf("must be a valid HTTP(S) URL")
		}
		return nil
	}
}

func ValidCron() Rule {
	return func(value any) error {
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a string")
		}
		if !cronPattern.MatchString(s) {
			return fmt.Errorf("must be a valid cron expression")
		}
		return nil
	}
}

func OneOf(allowed ...any) Rule {
	return func(value any) error {
		for _, a := range allowed {
			if fmt.Sprintf("%v", value) == fmt.Sprintf("%v", a) {
				return nil
			}
		}
		return fmt.Errorf("must be one of: %v", allowed)
	}
}

func Each(r Rule) Rule {
	return func(value any) error {
		arr, ok := value.([]any)
		if !ok {
			return fmt.Errorf("must be an array")
		}
		for i, item := range arr {
			if err := r(item); err != nil {
				return fmt.Errorf("[%d]: %v", i, err)
			}
		}
		return nil
	}
}
