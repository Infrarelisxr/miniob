#include "sql/operator/update_logical_operator.h"
#include "storage/field/field.h"

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, FieldMeta field, Value value)
    : table_(table), field_(field), value_(value)
{}