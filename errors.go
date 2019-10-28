package chainsync

import (
	"fmt"
)

var (
	ErrNamespaceExists = &catapultError{
		Status: "Failure_Namespace_Already_Exists",
		Description: "Validation failed because the namespace already exists.",
	}
)

var errs = []*catapultError{
	ErrNamespaceExists,
}

func ParseError(status string) error {
	for _, err := range errs {
		if err.Status == status {
			return err
		}
	}

	return &catapultError{
		Status: status,
		Description: fmt.Sprintf("Unnkown error: %s", status),
	}
}

type catapultError struct {
	Status string
	Description string
}

func (err *catapultError) Error() string {
	return err.Description
}