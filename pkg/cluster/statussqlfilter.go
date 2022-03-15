package cluster

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"

	"github.com/kyma-incubator/reconciler/pkg/db"
	"github.com/kyma-incubator/reconciler/pkg/model"
)

const (
	CreatedAtColumnName       = "Created"
	StatusCreatedAtColumnName = "StatusCreatedAt"
	StatusColumnName          = "Status"
	RuntimeIDColumnName       = "RuntimeID"
	ConfigIDColumnName        = "ConfigID"
)

type filterSQL struct {
	sql  string
	args []interface{}
}

type statusSQLFilter interface {
	Filter(dbType db.Type, colHdr *db.ColumnHandler) (*filterSQL, error)
}

type statusFilter struct {
	allowedStatuses []model.Status
}

func (sf *statusFilter) Filter(dbType db.Type, statusColHdr *db.ColumnHandler) (*filterSQL, error) {
	statusColName, err := statusColHdr.ColumnName(StatusColumnName)
	if err != nil {
		return &filterSQL{sql: "", args: []interface{}{}}, err
	}

	statuses := sf.statusesToStrings()

	var prefix rune
	switch dbType {
	case db.Postgres:
		prefix = ':'
	case db.SQLite:
		prefix = '@'
	}

	if len(statuses) > 1 {
		statusArgsPlaceholders, statusArgsNamedArgs := make([]string, len(statuses)), make([]interface{}, len(statuses))
		for i, status := range statuses {
			name := fmt.Sprintf("sf%v", i)
			statusArgsPlaceholders[i] = string(prefix) + name
			statusArgsNamedArgs[i] = sql.Named(name, status)
		}

		statusArgsNamedArgs = append(statusArgsNamedArgs, sql.Named("sfStatusColName", statusColName))

		return &filterSQL{
			sql:  fmt.Sprintf("%csfStatusColName IN (%v)", prefix, strings.Join(statusArgsPlaceholders, ",")),
			args: statusArgsNamedArgs,
		}, nil
	} else {
		return &filterSQL{
			sql: fmt.Sprintf("%s = %cstausFilterValue", statusColName, prefix),
			args: []interface{}{
				sql.Named("stausFilterValue", strings.Join(sf.statusesToStrings(), "','")),
			},
		}, nil
	}
}

func (sf *statusFilter) statusesToStrings() []string {
	var result []string
	for _, status := range sf.allowedStatuses {
		result = append(result, string(status))
	}
	return result
}

type reconcileIntervalFilter struct {
	reconcileInterval time.Duration
}

func (rif *reconcileIntervalFilter) Filter(dbType db.Type, statusColHdr *db.ColumnHandler) (*filterSQL, error) {
	statusColName, err := statusColHdr.ColumnName(StatusColumnName)
	if err != nil {
		return &filterSQL{sql: "", args: []interface{}{}}, err
	}
	createdColName, err := statusColHdr.ColumnName(StatusCreatedAtColumnName)
	if err != nil {
		return &filterSQL{sql: "", args: []interface{}{}}, err
	}
	switch dbType {
	case db.Postgres:
		return &filterSQL{
			sql: fmt.Sprintf(`%s IN (:rifCSR, :rifCSRER, :rifCSDER) AND :rifCN <= NOW() - INTERVAL :rifInterval`, statusColName),
			args: []interface{}{
				sql.Named("rifCSR", model.ClusterStatusReady),
				sql.Named("rifCSRER", model.ClusterStatusReconcileErrorRetryable),
				sql.Named("rifCSDER", model.ClusterStatusDeleteErrorRetryable),
				sql.Named("rifCN", createdColName),
				sql.Named("rifInterval", strconv.Itoa(int(rif.reconcileInterval.Seconds()))+" SECOND"),
			},
		}, nil
	case db.SQLite:
		return &filterSQL{
			sql: fmt.Sprintf(`%s IN (@rifCSR, @rifCSRER, @rifCSDER) AND @rifCN <= DATETIME('now', @rifInterval)`, statusColName),
			args: []interface{}{
				sql.Named("rifCSR", model.ClusterStatusReady),
				sql.Named("rifCSRER", model.ClusterStatusReconcileErrorRetryable),
				sql.Named("rifCSDER", model.ClusterStatusDeleteErrorRetryable),
				sql.Named("rifCN", createdColName),
				sql.Named("rifInterval", "-"+strconv.Itoa(int(rif.reconcileInterval.Seconds()))+" SECONDS"),
			},
		}, nil
	default:
		return &filterSQL{sql: "", args: []interface{}{}}, fmt.Errorf("database type '%s' is not supported by this filter", dbType)
	}
}

type createdIntervalFilter struct {
	runtimeID string
	interval  time.Duration
}

func (rif *createdIntervalFilter) Filter(dbType db.Type, statusColHdr *db.ColumnHandler) (*filterSQL, error) {
	runtimeIDColName, err := statusColHdr.ColumnName(RuntimeIDColumnName)
	if err != nil {
		return &filterSQL{sql: "", args: []interface{}{}}, err
	}
	createdColName, err := statusColHdr.ColumnName(CreatedAtColumnName)
	if err != nil {
		return &filterSQL{sql: "", args: []interface{}{}}, err
	}
	switch dbType {
	case db.Postgres:
		return &filterSQL{
			sql: fmt.Sprintf(`:cifRuntimeIDColName = :cifruntimeId AND :cifCreatedColName >= NOW() - INTERVAL :createdIntervalFilterInterval`),
			args: []interface{}{
				sql.Named("cifRuntimeIDColName", runtimeIDColName),
				sql.Named("cifruntimeId", rif.runtimeID),
				sql.Named("cifCreatedColName", createdColName),
				sql.Named("createdIntervalFilterInterval", strconv.Itoa(int(rif.interval.Seconds()))+" SECOND"),
			},
		}, nil
	case db.SQLite:
		return &filterSQL{
			sql: fmt.Sprintf(`@cifRuntimeIDColName = @cifruntimeId AND @cifCreatedColName >= DATETIME('now', @createdIntervalFilterInterval)`),
			args: []interface{}{
				sql.Named("cifRuntimeIDColName", runtimeIDColName),
				sql.Named("cifruntimeId", rif.runtimeID),
				sql.Named("cifCreatedColName", createdColName),
				sql.Named("createdIntervalFilterInterval", "-"+strconv.FormatFloat(rif.interval.Seconds(), 'f', -1, 64)+" SECONDS"),
			},
		}, nil
	default:
		return &filterSQL{sql: "", args: []interface{}{}}, fmt.Errorf("database type '%s' is not supported by this filter", dbType)
	}
}

type runtimeIDFilter struct {
	runtimeID string
}

func namedParameterPrefix(dbType db.Type) rune {
	switch dbType {
	case db.Postgres:
		return ':'
	case db.SQLite:
		return '@'
	default:
		panic(errors.New("cannot create named parameter prefix from " + string(dbType)))
	}
}

func (r *runtimeIDFilter) Filter(dbType db.Type, statusColHdr *db.ColumnHandler) (*filterSQL, error) {
	runtimeIDColName, err := statusColHdr.ColumnName(RuntimeIDColumnName)
	if err != nil {
		return &filterSQL{sql: "", args: []interface{}{}}, err
	}

	switch dbType {
	case db.Postgres:
		return &filterSQL{
			sql: ":ridfRuntimeIDColName = :ridfRuntimeId",
			args: []interface{}{
				sql.Named("ridfRuntimeIDColName", runtimeIDColName),
				sql.Named("ridfRuntimeId", r.runtimeID),
			},
		}, nil
	case db.SQLite:
		return &filterSQL{
			sql: "@ridfRuntimeIDColName = @ridfRuntimeId",
			args: []interface{}{
				sql.Named("ridfRuntimeIDColName", runtimeIDColName),
				sql.Named("ridfRuntimeId", r.runtimeID),
			},
		}, nil
	default:
		return &filterSQL{sql: "", args: []interface{}{}}, fmt.Errorf("database type '%s' is not supported by this filter", dbType)
	}
}

type configIDFilter struct {
	configID int64
}

func (r *configIDFilter) Filter(dbType db.Type, statusColHdr *db.ColumnHandler) (*filterSQL, error) {
	configIDColName, err := statusColHdr.ColumnName(ConfigIDColumnName)
	if err != nil {
		return &filterSQL{sql: "", args: []interface{}{}}, err
	}

	switch dbType {
	case db.Postgres:
		return &filterSQL{
			sql: "? = ?",
			args: []interface{}{
				configIDColName,
				r.configID,
			},
		}, nil
	case db.SQLite:
		return &filterSQL{
			sql: "@cidfConfigIDColName = @cidfConfigId",
			args: []interface{}{
				sql.Named("cidfConfigIDColName", configIDColName),
				sql.Named("cidfConfigId", r.configID),
			},
		}, nil
	default:
		return &filterSQL{sql: "", args: []interface{}{}}, fmt.Errorf("database type '%s' is not supported by this filter", dbType)
	}
}
