/*-------------------------------------------------------------------------
 *
 * pg_tracing_explain.c
 * 		pg_tracing plan explain functions.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "parser/parsetree.h"
#include "nodes/extensible.h"
#include "pg_tracing.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"


/*
 * Add the target relation of a scan or modify node to the stringinfo
 */
static void
ExplainTargetRel(const planstateTraceContext * planstateTraceContext, const PlanState *planstate, Index rti, StringInfo str)
{
	char	   *objectname = NULL;
	char	   *namespace = NULL;
	RangeTblEntry *rte;
	char	   *refname;

	List	   *rtable;

	rtable = planstate->state->es_range_table;
	rte = rt_fetch(rti, rtable);

	refname = (char *) list_nth(planstateTraceContext->rtable_names, rti - 1);
	if (refname == NULL)
		refname = rte->eref->aliasname;

	switch (nodeTag(planstate->plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_ModifyTable:
			/* Assert it's on a real relation */
			Assert(rte->rtekind == RTE_RELATION);
			objectname = get_rel_name(rte->relid);
			break;
		case T_FunctionScan:
			{
				FunctionScan *fscan = (FunctionScan *) planstate->plan;

				/* Assert it's on a RangeFunction */
				Assert(rte->rtekind == RTE_FUNCTION);

				/*
				 * If the expression is still a function call of a single
				 * function, we can get the real name of the function.
				 * Otherwise, punt.  (Even if it was a single function call
				 * originally, the optimizer could have simplified it away.)
				 */
				if (list_length(fscan->functions) == 1)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(fscan->functions);

					if (IsA(rtfunc->funcexpr, FuncExpr))
					{
						FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
						Oid			funcid = funcexpr->funcid;

						objectname = get_func_name(funcid);
					}
				}
			}
			break;
		case T_TableFuncScan:
			Assert(rte->rtekind == RTE_TABLEFUNC);
			objectname = "xmltable";
			break;
		case T_ValuesScan:
			Assert(rte->rtekind == RTE_VALUES);
			break;
		case T_CteScan:
			/* Assert it's on a non-self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(!rte->self_reference);
			objectname = rte->ctename;
			break;
		case T_NamedTuplestoreScan:
			Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);
			objectname = rte->enrname;
			break;
		case T_WorkTableScan:
			/* Assert it's on a self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(rte->self_reference);
			objectname = rte->ctename;
			break;
		default:
			break;
	}

	appendStringInfoString(str, " on");
	if (namespace != NULL)
		appendStringInfo(str, " %s.%s", quote_identifier(namespace),
						 quote_identifier(objectname));
	else if (objectname != NULL)
		appendStringInfo(str, " %s", quote_identifier(objectname));
	if (objectname == NULL || strcmp(refname, objectname) != 0)
		appendStringInfo(str, " %s", quote_identifier(refname));
}

/*
 * Add the target of a Scan node to the stringinfo
 */
static void
ExplainScanTarget(const planstateTraceContext * planstateTraceContext, const PlanState *planstate, const Scan *plan, StringInfo str)
{
	ExplainTargetRel(planstateTraceContext, planstate, plan->scanrelid, str);
}

/*
 * Add the target of a ModifyTable node to the stringinfo
 */
static void
ExplainModifyTarget(const planstateTraceContext * planstateTraceContext, const PlanState *planstate, const ModifyTable *plan, StringInfo str)
{
	ExplainTargetRel(planstateTraceContext, planstate, plan->nominalRelation, str);
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan to the stringinfo
 */
static void
ExplainIndexScanDetails(Oid indexId, ScanDirection indexorderdir,
						StringInfo str)
{
	const char *indexname = get_rel_name(indexId);

	if (ScanDirectionIsBackward(indexorderdir))
		appendStringInfoString(str, " Backward");
	appendStringInfo(str, " using %s", quote_identifier(indexname));
}

/*
 * Generate a relation name from a planstate and add it to the stringinfo
 */
static void
plan_to_rel_name(const PlanState *planstate, const planstateTraceContext * planstateTraceContext, StringInfo str)
{
	const Plan *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
			ExplainScanTarget(planstateTraceContext, planstate, (Scan *) plan, str);
			break;
		case T_ForeignScan:
		case T_CustomScan:
			if (((Scan *) plan)->scanrelid > 0)
				ExplainScanTarget(planstateTraceContext, planstate, (Scan *) plan, str);
			break;
		case T_IndexScan:
			{
				IndexScan  *indexscan = (IndexScan *) plan;

				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										str);
				ExplainScanTarget(planstateTraceContext, planstate, (Scan *) indexscan, str);
			}
			break;
		case T_IndexOnlyScan:
			{
				IndexOnlyScan *indexonlyscan = (IndexOnlyScan *) plan;

				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										str);
				ExplainScanTarget(planstateTraceContext, planstate, (Scan *) indexonlyscan, str);
			}
			break;
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmapindexscan = (BitmapIndexScan *) plan;
				const char *indexname = get_rel_name(bitmapindexscan->indexid);

				appendStringInfo(str, " on %s",
								 quote_identifier(indexname));
			}
			break;
		case T_ModifyTable:
			ExplainModifyTarget(planstateTraceContext, planstate, (ModifyTable *) plan, str);
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				const char *jointype;

				switch (((Join *) plan)->jointype)
				{
					case JOIN_INNER:
						jointype = "Inner";
						break;
					case JOIN_LEFT:
						jointype = "Left";
						break;
					case JOIN_FULL:
						jointype = "Full";
						break;
					case JOIN_RIGHT:
						jointype = "Right";
						break;
					case JOIN_SEMI:
						jointype = "Semi";
						break;
					case JOIN_ANTI:
						jointype = "Anti";
						break;
#if PG_VERSION_NUM > 160000
					case JOIN_RIGHT_ANTI:
						jointype = "Right Anti";
						break;
#endif
					default:
						jointype = "???";
						break;
				}
				if (((Join *) plan)->jointype != JOIN_INNER)
					appendStringInfo(str, " %s Join", jointype);
				else if (!IsA(plan, NestLoop))
					appendStringInfoString(str, " Join");
			}
			break;
		case T_SetOp:
			{
				const char *setopcmd;

				switch (((SetOp *) plan)->cmd)
				{
					case SETOPCMD_INTERSECT:
						setopcmd = "Intersect";
						break;
					case SETOPCMD_INTERSECT_ALL:
						setopcmd = "Intersect All";
						break;
					case SETOPCMD_EXCEPT:
						setopcmd = "Except";
						break;
					case SETOPCMD_EXCEPT_ALL:
						setopcmd = "Except All";
						break;
					default:
						setopcmd = "???";
						break;
				}
				appendStringInfo(str, " %s", setopcmd);
			}
			break;
		default:
			break;
	}
}

/*
 * Generate scan qualifier from deparsed expression and add it to stringinfo
 */
static void
add_scan_qual(StringInfo str, const PlanState *planstate, List *qual,
			  const char *qlabel, List *ancestors, List *deparse_ctx,
			  bool useprefix)
{
	List	   *context;
	char	   *exprstr;
	Node	   *node;

	if (qual == NIL)
		return;

	node = (Node *) make_ands_explicit(qual);

	Assert(deparse_ctx != NULL);
	/* Set up deparsing context */
	context = set_deparse_context_plan(deparse_ctx,
									   planstate->plan, ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);
	if (str->len > 0)
		appendStringInfoChar(str, '|');
	appendStringInfoString(str, qlabel);
	appendStringInfoString(str, exprstr);
}

const char *
plan_to_deparse_info(const planstateTraceContext * planstateTraceContext, const PlanState *planstate)
{
	StringInfo	deparse_info = makeStringInfo();
	Plan const *plan = planstate->plan;
	List	   *ancestors = planstateTraceContext->ancestors;
	List	   *deparse_ctx = planstateTraceContext->deparse_ctx;

	if (deparse_ctx == NULL)
		return "";

	switch (nodeTag(plan))
	{
		case T_IndexScan:
			add_scan_qual(deparse_info, planstate, ((IndexScan *) plan)->indexqualorig, "Index Cond: ",
						  ancestors, deparse_ctx, false);
			break;
		case T_IndexOnlyScan:
			add_scan_qual(deparse_info, planstate, ((IndexOnlyScan *) plan)->indexqual, "Index Cond: ",
						  ancestors, deparse_ctx, false);
			break;
		case T_BitmapIndexScan:
			add_scan_qual(deparse_info, planstate, ((BitmapIndexScan *) plan)->indexqualorig, "Index Cond: ",
						  ancestors, deparse_ctx, false);
			break;
		case T_BitmapHeapScan:
			add_scan_qual(deparse_info, planstate, ((BitmapHeapScan *) plan)->bitmapqualorig, "Recheck Cond: ",
						  ancestors, deparse_ctx, false);
			break;
		case T_SeqScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_SubqueryScan:
		case T_Gather:
		case T_GatherMerge:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_TidRangeScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_Agg:
		case T_WindowAgg:
		case T_Group:
		case T_Result:
			add_scan_qual(deparse_info, planstate, plan->qual, "Filter : ",
						  ancestors, deparse_ctx, false);
			break;
		case T_NestLoop:
			add_scan_qual(deparse_info, planstate, ((NestLoop *) plan)->join.joinqual, "Join Filter : ",
						  ancestors, deparse_ctx, false);
			add_scan_qual(deparse_info, planstate, plan->qual, "Filter : ",
						  ancestors, deparse_ctx, false);
			break;
		case T_MergeJoin:
			add_scan_qual(deparse_info, planstate, ((MergeJoin *) plan)->mergeclauses, "Merge Cond: ",
						  ancestors, deparse_ctx, false);
			add_scan_qual(deparse_info, planstate, ((MergeJoin *) plan)->join.joinqual, "Join Filter: ",
						  ancestors, deparse_ctx, false);
			add_scan_qual(deparse_info, planstate, plan->qual, "Filter : ",
						  ancestors, deparse_ctx, false);
			break;
		case T_HashJoin:
			add_scan_qual(deparse_info, planstate, ((HashJoin *) plan)->hashclauses, "Hash Cond: ",
						  ancestors, deparse_ctx, false);
			add_scan_qual(deparse_info, planstate, ((MergeJoin *) plan)->join.joinqual, "Join Filter: ",
						  ancestors, deparse_ctx, false);
			add_scan_qual(deparse_info, planstate, plan->qual, "Filter : ",
						  ancestors, deparse_ctx, false);
			break;
		default:
			break;
	}
	return deparse_info->data;
}

/*
 * Generate an operation name from a planstate
 */
char const *
plan_to_operation(const planstateTraceContext * planstateTraceContext, const PlanState *planstate, const char *span_type)
{
	StringInfo	operation_name = makeStringInfo();
	Plan const *plan = planstate->plan;

	if (plan->parallel_aware)
		appendStringInfoString(operation_name, "Parallel ");
	if (plan->async_capable)
		appendStringInfoString(operation_name, "Async ");
	appendStringInfoString(operation_name, span_type);

	plan_to_rel_name(planstate, planstateTraceContext, operation_name);

	return operation_name->data;
}

/*
 * Get the node type name from a plan node
 */
const char *
plan_to_node_type(const Plan *plan)
{
	const char *custom_name;

	switch (nodeTag(plan))
	{
		case T_Result:
			return "Result";
		case T_ProjectSet:
			return "ProjectSet";
		case T_ModifyTable:
			switch (((ModifyTable *) plan)->operation)
			{
				case CMD_INSERT:
					return "Insert";
				case CMD_UPDATE:
					return "Update";
				case CMD_DELETE:
					return "Delete";
				case CMD_MERGE:
					return "Merge";
				default:
					return "???";
			}
		case T_Append:
			return "Append";
		case T_MergeAppend:
			return "MergeAppend";
		case T_RecursiveUnion:
			return "RecursiveUnion";
		case T_BitmapAnd:
			return "BitmapAnd";
		case T_BitmapOr:
			return "BitmapOr";
		case T_NestLoop:
			return "NestedLoop";
		case T_MergeJoin:
			return "Merge";		/* "Join" gets added by jointype switch */
		case T_HashJoin:
			return "Hash";		/* "Join" gets added by jointype switch */
		case T_SeqScan:
			return "SeqScan";
		case T_SampleScan:
			return "SampleScan";
		case T_Gather:
			return "Gather";
		case T_GatherMerge:
			return "GatherMerge";
		case T_IndexScan:
			return "IndexScan";
		case T_IndexOnlyScan:
			return "IndexOnlyScan";
		case T_BitmapIndexScan:
			return "BitmapIndexScan";
		case T_BitmapHeapScan:
			return "BitmapHeapScan";
		case T_TidScan:
			return "TidScan";
		case T_TidRangeScan:
			return "TidRangeScan";
		case T_SubqueryScan:
			return "SubqueryScan";
		case T_FunctionScan:
			return "FunctionScan";
		case T_TableFuncScan:
			return "TableFunctionScan";
		case T_ValuesScan:
			return "ValuesScan";
		case T_CteScan:
			return "CTEScan";
		case T_NamedTuplestoreScan:
			return "NamedTuplestoreScan";
		case T_WorkTableScan:
			return "WorkTableScan";
		case T_ForeignScan:
			switch (((ForeignScan *) plan)->operation)
			{
				case CMD_SELECT:
					return "ForeignScan";
				case CMD_INSERT:
					return "ForeignInsert";
				case CMD_UPDATE:
					return "ForeignUpdate";
				case CMD_DELETE:
					return "ForeignDelete";
				default:
					return "???";
			}
		case T_CustomScan:
			custom_name = ((CustomScan *) plan)->methods->CustomName;
			if (custom_name)
				return psprintf("CustomScan (%s)", custom_name);
			else
				return "CustomScan";
		case T_Material:
			return "Materialize";
		case T_Memoize:
			return "Memoize";
		case T_Sort:
			return "Sort";
		case T_IncrementalSort:
			return "IncrementalSort";
		case T_Group:
			return "Group";
		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;

				switch (agg->aggstrategy)
				{
					case AGG_PLAIN:
						return "Aggregate";
					case AGG_SORTED:
						return "GroupAggregate";
					case AGG_HASHED:
						return "HashAggregate";
					case AGG_MIXED:
						return "MixedAggregate";
					default:
						return "Aggregate ???";
				}
			}
		case T_WindowAgg:
			return "WindowAgg";
		case T_Unique:
			return "Unique";
		case T_SetOp:
			switch (((SetOp *) plan)->strategy)
			{
				case SETOP_SORTED:
					return "SetOp";
				case SETOP_HASHED:
					return "HashSetOp";
				default:
					return "SetOp ???";
			}
		case T_LockRows:
			return "LockRows";
		case T_Limit:
			return "Limit";
		case T_Hash:
			return "Hash";
		default:
			return "???";
	}
}
