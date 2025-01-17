package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/avast/retry-go"
	"github.com/kyma-incubator/reconciler/pkg/cluster"
	"github.com/kyma-incubator/reconciler/pkg/model"
	"github.com/kyma-incubator/reconciler/pkg/scheduler/invoker"
	"github.com/kyma-incubator/reconciler/pkg/scheduler/reconciliation"
	"github.com/kyma-incubator/reconciler/pkg/scheduler/reconciliation/operation"
	"go.uber.org/zap"
)

type worker struct {
	reconRepo  reconciliation.Repository
	invoker    invoker.Invoker
	logger     *zap.SugaredLogger
	maxRetries int
	retryDelay time.Duration
}

func (w *worker) run(ctx context.Context, clusterState *cluster.State, op *model.OperationEntity, maxOpRetries int) error {
	if !w.isProcessable(op) {
		w.logger.Warnf("Worker cannot start processing of operation '%s' because it is in non-processable state '%s'",
			op, op.State)
		return nil
	}

	w.logger.Debugf("Worker starts processing of operation '%s'", op)

	compsReady, err := w.componentsReady(op)
	if err != nil {
		return err
	}

	comp := clusterState.Configuration.GetComponent(op.Component)
	if comp == nil {
		return fmt.Errorf("cluster '%s' has no component '%s' configured",
			clusterState.Cluster.RuntimeID, op.Component)
	}

	retryable := func() error {
		w.logger.Debugf("Worker calls invoker for operation '%s' (in retryable function)", op)
		return w.invoker.Invoke(ctx, &invoker.Params{
			ComponentToReconcile: comp,
			ComponentsReady:      compsReady,
			SchedulingID:         op.SchedulingID,
			CorrelationID:        op.CorrelationID,
			ClusterState:         clusterState,
			MaxOperationRetries:  maxOpRetries,
			Type:                 op.Type,
		})
	}

	//retry calling the invoker if error was returned
	err = retry.Do(retryable,
		retry.Attempts(uint(w.maxRetries)),
		retry.Delay(w.retryDelay),
		retry.LastErrorOnly(false),
		retry.Context(ctx))

	if err == nil {
		w.logger.Debugf("Worker finished processing of operation '%s' successfully", op)
	} else {
		w.logger.Warnf("Worker stops processing operation '%s' because invoker "+
			"returned consistently errors (%d retries): %s", op, w.maxRetries, err)
	}
	return err
}

func (w *worker) componentsReady(op *model.OperationEntity) ([]string, error) {
	opsReady, err := w.reconRepo.GetOperations(&operation.FilterMixer{
		Filters: []operation.Filter{
			&operation.WithSchedulingID{SchedulingID: op.SchedulingID},
			&operation.WithStates{States: []model.OperationState{model.OperationStateDone}},
		},
	})
	if err != nil {
		return nil, err
	}
	var result []string
	for _, opReady := range opsReady {
		result = append(result, opReady.Component)
	}
	return result, nil
}

func (w *worker) isProcessable(op *model.OperationEntity) bool {
	return op.State != model.OperationStateDone &&
		op.State != model.OperationStateError &&
		op.State != model.OperationStateInProgress
}
