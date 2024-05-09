#include "sql/operator/aggregate_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC AggregatePhysicalOperator::open(Trx *trx)
{
if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC AggregatePhysicalOperator::next()
{
    // already aggregated
    if (result_tuple_.cell_num() > 0){
        return RC::RECORD_EOF;
    }

    RC rc = RC::SUCCESS;
    PhysicalOperator *oper = children_[0].get();
    std::vector<Value> result_cells;
    int Nums_Of_Tuples = 0;
    while (RC::SUCCESS == (rc = oper->next())) {

        Tuple *tuple = oper->current_tuple();
        Nums_Of_Tuples += 1;

        for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {
            const AggrOp aggregation = aggregations_[cell_idx];

            Value cell;
            AttrType attr_type = AttrType::INTS;
            switch (aggregation)
            {
            case AggrOp::AGGR_SUM:
                rc = tuple->cell_at(cell_idx, cell);
                attr_type = cell.attr_type();
                if(attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
                   if(static_cast<int>(result_cells.size())!=(int)aggregations_.size()){
                    result_cells.push_back(cell);
                  }else{
                    result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
                  }
                }
                break;
            case AggrOp::AGGR_MAX:
                rc = tuple->cell_at(cell_idx, cell);
                attr_type = cell.attr_type();
                if(static_cast<int>(result_cells.size())!=(int)aggregations_.size()){
                  result_cells.push_back(cell);
                }
                else if(result_cells[cell_idx].compare(cell)==-1){

                  result_cells[cell_idx].set_value(cell);
                }
                break;
            case AggrOp::AGGR_MIN:
                rc = tuple->cell_at(cell_idx, cell);
                attr_type = cell.attr_type();
                if(static_cast<int>(result_cells.size())!=(int)aggregations_.size()){
                  result_cells.push_back(cell);
                }
                else if(result_cells[cell_idx].compare(cell)==1){

                  result_cells[cell_idx].set_value(cell);
                }
                break;
            case AggrOp::AGGR_AVG:
            case AggrOp::AGGR_COUNT_ALL:
            case AggrOp::AGGR_COUNT:
                rc = tuple->cell_at(cell_idx, cell);
                if(static_cast<int>(result_cells.size())!=(int)aggregations_.size()){
                  result_cells.push_back(cell);
              }
                break;    
            default:
                return RC::UNIMPLENMENT;
            }
        }
    }

    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++){
      if(aggregations_[cell_idx]==AggrOp::AGGR_AVG){
        result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()/Nums_Of_Tuples);
      }
      if(aggregations_[cell_idx]== AggrOp::AGGR_COUNT || aggregations_[cell_idx] == AggrOp::AGGR_COUNT_ALL){
          result_cells[cell_idx].set_float((float)Nums_Of_Tuples);
        }
    }
    if (rc == RC::RECORD_EOF){
        rc = RC::SUCCESS;
    }
    result_tuple_.set_cells(result_cells);
    return rc;
}

RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
Tuple *AggregatePhysicalOperator::current_tuple()
{
  return &result_tuple_;
}