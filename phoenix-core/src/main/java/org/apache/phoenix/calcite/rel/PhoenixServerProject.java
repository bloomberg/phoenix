package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.PhoenixSequence;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementor.ImplementorContext;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjector;

import com.google.common.base.Supplier;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.Sequence;

public class PhoenixServerProject extends PhoenixAbstractProject {
    
    public static PhoenixServerProject create(final RelNode input, 
            final List<? extends RexNode> projects, RelDataType rowType) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.SERVER)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.project(mq, input, projects);
                    }
                });
        return new PhoenixServerProject(cluster, traits, input, projects, rowType);
    }

    private PhoenixServerProject(RelOptCluster cluster, RelTraitSet traits,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public PhoenixServerProject copy(RelTraitSet traits, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return create(input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.SERVER))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq)
                .multiplyBy(SERVER_FACTOR)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().retainPKColumns, false, getColumnRefList()));
        QueryPlan plan = implementor.visitInput(0, (PhoenixQueryRel) getInput());
        implementor.popContext();
        
        assert (plan instanceof ScanPlan);

        PhoenixSequence sequence = CalciteUtils.findSequence(this);
        final SequenceManager seqManager = sequence == null ?
                null : new SequenceManager(new PhoenixStatement(sequence.pc));
        implementor.setSequenceManager(seqManager);
        TupleProjector tupleProjector = super.project(implementor);
        if (seqManager != null) {
            try {
                seqManager.validateSequences(Sequence.ValueOp.VALIDATE_SEQUENCE);
                StatementContext context = new StatementContext(
                        plan.getContext().getStatement(),
                        plan.getContext().getResolver(),
                        new Scan(), seqManager);
                plan = new ClientScanPlan(
                        context, plan.getStatement(), plan.getTableRef(),
                        RowProjector.EMPTY_PROJECTOR, null, null, null,
                        OrderByCompiler.OrderBy.EMPTY_ORDER_BY, plan);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        TupleProjector.serializeProjectorIntoScan(plan.getContext().getScan(), tupleProjector);
        return plan;
    }
}
