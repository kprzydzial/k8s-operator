import pytest
from backup_operator.operation import Operation
from backup_operator.operation_strategy import get_strategy
from backup_operator.zalando_operation_strategy import ZalandoBackup, ZalandoRestore
from backup_operator.cnpg_operation_strategy import CNPGBackup, CNPGRestore

def test_get_strategy_zalando_backup():
    spec = {"operator": "zalando", "action": "backup"}
    operation = Operation("test-op", "default", spec, {})
    operator = object()
    logger = object()
    strategy = get_strategy(operator, operation, logger)
    assert isinstance(strategy, ZalandoBackup)
    assert strategy.operator == operator
    assert strategy.operation == operation
    assert strategy.logger == logger

def test_get_strategy_zalando_restore():
    spec = {"operator": "zalando", "action": "restore"}
    operation = Operation("test-op", "default", spec, {})
    operator = object()
    logger = object()
    strategy = get_strategy(operator, operation, logger)
    assert isinstance(strategy, ZalandoRestore)
    assert strategy.operator == operator
    assert strategy.operation == operation
    assert strategy.logger == logger

def test_get_strategy_cnpg_backup():
    spec = {"operator": "cnpg", "action": "backup"}
    operation = Operation("test-op", "default", spec, {})
    operator = object()
    logger = object()
    strategy = get_strategy(operator, operation, logger)
    assert isinstance(strategy, CNPGBackup)
    assert strategy.operator == operator
    assert strategy.operation == operation
    assert strategy.logger == logger

def test_get_strategy_cnpg_restore():
    spec = {"operator": "cnpg", "action": "restore"}
    operation = Operation("test-op", "default", spec, {})
    operator = object()
    logger = object()
    strategy = get_strategy(operator, operation, logger)
    assert isinstance(strategy, CNPGRestore)
    assert strategy.operator == operator
    assert strategy.operation == operation
    assert strategy.logger == logger

def test_get_strategy_unknown_operator():
    spec = {"operator": "unknown", "action": "backup"}
    operation = Operation("test-op", "default", spec, {})
    operator = object()
    logger = object()
    with pytest.raises(ValueError, match="Unknown operator/action: unknown/backup"):
        get_strategy(operator, operation, logger)

def test_get_strategy_defaults_to_zalando_backup():
    # Operation defaults to zalando backup if not specified
    spec = {}
    operation = Operation("test-op", "default", spec, {})
    operator = object()
    logger = object()
    strategy = get_strategy(operator, operation, logger)
    assert isinstance(strategy, ZalandoBackup)
    assert strategy.operator == operator
    assert strategy.operation == operation
    assert strategy.logger == logger
