package db

import (
	"database/sql"
	"fmt"
	"strings"
)

var postgresNamedParameterPrefix = ':'
var postgresReplacementPositionalArgPrefix = '$'

//convertNamedToPositionalArgs exists because we want the ability of named parameterized queries but postgres only supports
//positional arguments
func (pc *postgresConnection) convertNamedToPositionalArgs(query string, args ...interface{}) (string, []interface{}, error) {
	freqMap := make(map[string]*int)

	for i, arg := range args {

		arg, ok := arg.(sql.NamedArg)
		if !ok {
			continue
		}

		if freqMap[arg.Name] != nil {
			existingPosition := *freqMap[arg.Name]
			existingArg := args[existingPosition].(sql.NamedArg)
			o := fmt.Sprintf("%c%s", postgresNamedParameterPrefix, existingArg.Name)
			n := fmt.Sprintf("%c%v", postgresReplacementPositionalArgPrefix, existingPosition)
			query = strings.Replace(query, o, n, -1)
			pc.logger.Debugf("%s >> %s (preexisting)", o, n)
			continue
		}

		p := i + 1
		o, n := fmt.Sprintf(":%s", arg.Name), fmt.Sprintf("$%v", p)
		query = strings.Replace(query, o, n, -1)
		args[i] = arg.Value
		freqMap[arg.Name] = &p
		pc.logger.Debugf("%s >> %s", o, n)
	}

	return query, args, nil
}
