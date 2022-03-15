package db

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"reflect"
	"testing"
)

func TestReplacementOfNamedWithPositionalArgs(t *testing.T) {
	cn := postgresConnection{
		logger: zap.NewExample().Sugar(),
	}

	testCases := []struct {
		name                        string
		sql                         string
		params                      []interface{}
		expectedSql                 string
		expectedParams              []interface{}
		expectMixupError            bool
		expectNamedArgNotFoundError bool
	}{
		{
			name: "simple check",
			sql:  "SELECT * FROM :table WHERE :v1 = :v2 AND :v3 = :v3",
			params: []interface{}{
				sql.Named("table", "SOME_TABLE"),
				sql.Named("v1", "VALUE_1"),
				sql.Named("v2", "VALUE_2"),
				sql.Named("v3", "VALUE_3"),
			},
			expectedSql: "SELECT * FROM $1 WHERE $2 = $3 AND $4 = $4",
			expectedParams: []interface{}{
				"SOME_TABLE",
				"VALUE_1",
				"VALUE_2",
				"VALUE_3",
			},
		},
		{
			name: "mixed named and positional args lead to error",
			sql:  "SELECT * FROM :table WHERE :v1 = :v2 AND :v3 IN ?",
			params: []interface{}{
				sql.Named("v2", "VALUE_2"),
				sql.Named("v3", "VALUE_3"),
				sql.Named("v1", "VALUE_1"),
				"SOME_TABLE",
			},
			expectMixupError: true,
		},
		{
			name: "forgotten named argument leads to error",
			sql:  "SELECT * FROM :table WHERE :v1 = :v2 AND :v3 = :v3",
			params: []interface{}{
				sql.Named("table", "SOME_TABLE"),
				sql.Named("v1", "VALUE_1"),
				sql.Named("v2", "VALUE_2"),
			},
			expectNamedArgNotFoundError: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rsql, rparams, err := cn.convertNamedToPositionalArgs(testCase.sql, testCase.params...)
			if testCase.expectMixupError {
				require.Error(t, err)
			} else if testCase.expectNamedArgNotFoundError {

			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedSql, rsql, "sql must match")
				require.True(t, reflect.DeepEqual(testCase.params, rparams), "replaced parameters must match")
			}
		})
	}
}
