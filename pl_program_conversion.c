#include "plpgsql.h"
#include "nodes/pg_list.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <igraph/igraph.h>
#include "pl_program_conversion.h"
#include "postgres.h"
#include "nodes/nodes.h"
#include "parser/gramparse.h"
#include "parser/parser.h"
#include "nodes/pg_list.h"
#include "catalog/pg_type.h"
#include "pl_graphs/pl_graphs.h"

// TODO: Split in different files
char *valToStringEscape(Value  *val,
                        bool   escape);

char *valToString(Value *val);

void replaceStmt(PLpgSQL_stmt  *oldStmt,
                 PLpgSQL_stmt  *newStmt,
                 List          *parents);

char *concatMultipleValues(List  *values1,
                           List  *values2,
                           char  *infix,
                           char  *suffix,
                           char  *prefix1,
                           char  *prefix2);

char *concatValuesWithPrefix(List  *values,
                             char  *seperator,
                             char  *prefix);

char *concatValues(List  *values,
                   char  *seperator);

char *resTargetToString(ResTarget  *target,
                        bool       useAliasIfPresent);

List *resolveQueryRefsSkipNFull(char  *query,
                                int   n);

List *resolveQueryRefsSkipN(char  *query,
                            int   n);

List *resolveQueryRefs(char *query);

Value *resolveTargetRefStrToCorrespVarno(   int  varno,
                                            int  *vars,
                                            int  nvars,
                                            char *query);

List *resolveBmsRefs(Bitmapset          *bms_in);

Value *makeStringWithAlias(char  *string,
                           int   aliasId);

List *resolveQueryColumns(Bitmapset          *bms_in,
                          int                *vars,
                          int                nvars,
                          char               *query,
                          PLpgSQL_execstate  *estate,
                          PLpgSQL_function   *func,
                          bool               aliases);

char *qb(char  *queryToBatch,
         char  *parameterQuery);

char *projectWithAlias(char  *baseQuery,
                       char  *newSelects,
                       char  *subQueryAlias);

char *project(char  *baseQuery,
              char  *newSelects);

char *leftOuterJoin(char  *baseQuery,
                    char  *secondQuery,
                    List  *params,
                    char  *aliasLeft,
                    char  *aliasRight);

PLpgSQL_stmt_fors *createFors(  char *query,
                                PLpgSQL_rec *rec,
                                PLpgSQL_row *row,
                                List *body,
                                PLpgSQL_function *func);

void handleFors(PLpgSQL_stmt_fors *fors ,
                List               *parents,
                PLpgSQL_execstate  *estate,
                PLpgSQL_function   *func);


PLpgSQL_stmt_assign *createQueryAssignment( int varno,
                                            char* query) ;

List *resolveQueryColumnsWithRecord(Bitmapset          *bms_in,
                                    PLpgSQL_rec *      record,
                                    char               *query,
                                    PLpgSQL_execstate  *estate,
                                    PLpgSQL_function   *func,
                                    bool               aliases);

PLpgSQL_rec* createRecordAndAddToNamespace(char* label);

/* Function memory context*/
MemoryContext func_cxt;

/** General backend operations **/

/**
 * converts a Value* to a character String
 * @param  val    the input Value Pointer
 * @param  escape if it is a string Value escape it?
 * @return        character String
 */
char *valToStringEscape(Value  *val,
                        bool   escape) {
    char *value = palloc0(64);
    switch (val->type) {
    case T_String:
        if (escape) {
            sprintf(value, "'%s'", strVal(val));
        } else {
            sprintf(value, "%s", strVal(val));
        }
        break;
    case T_Float:
        sprintf(value, "%f", floatVal(val));
        break;
    case T_Integer:
        sprintf(value, "%li", intVal(val));
        break;
    default:
        elog(ERROR, "Unkown node type:%s\n", nodeToString(val));
        break;
    }
    return value;
}

/**
 * converts a Value* to a character String
 * @param  val the input Value pointer
 * @return     character String
 */
char *valToString(Value *val) {
    return valToStringEscape(val, 0);
}

/**
 * cancatinates multiple List Values to a character String
 * @param  values1 first list
 * @param  values2 second list
 * @param  infix   infix characters
 * @param  suffix  suffix character
 * @param  prefix1 prefix for values1
 * @param  prefix2 prefix for values2
 * @return         concatinated character string
 */
char *concatMultipleValues(List  *values1,
                           List  *values2,
                           char  *infix,
                           char  *suffix,
                           char  *prefix1,
                           char  *prefix2) {
    char *vals       = palloc0(64);
    ListCell *value1 = values1->head;
    ListCell *value2 = values2->head;
    while (value1 && value2) {
        char *val1 = valToString(lfirst(value1));
        char *val2 = valToString(lfirst(value2));
        sprintf(eos(vals), "%s.%s %s %s.%s", prefix1, val1, infix, prefix2, val2);
        if (value1->next && value2->next)
            sprintf(eos(vals), " %s ", suffix);
        value1 = value1->next;
        value2 = value2->next;
    }
    return vals;
}

/**
 * cancatinates multiple List Values to a character String
 * @param  values    input list
 * @param  seperator infix seperator
 * @param  prefix    prefix for the list elements
 * @return           concatinated character string
 */
char *concatValuesWithPrefix(List  *values,
                             char  *seperator,
                             char  *prefix) {
    char *vals = palloc0(64);
    ListCell *cell;
    foreach(cell, values) {
        if (prefix)
            sprintf(eos(vals), "%s.", prefix);
        sprintf(eos(vals), "%s", valToString(lfirst(cell)));
        if (cell->next)
            sprintf(eos(vals), "%s", seperator);
    }
    return vals;
}

/**
 * cancatinates multiple List Values to a character String
 * @param  values    input list
 * @param  seperator infix seperator
 * @return           concatinated character string
 */
char *concatValues(List  *values,
                   char  *seperator) {
    return concatValuesWithPrefix(values, seperator, NULL);
}

/**
 * Converts a restarget pointer to a string
 * @param  target the restarget pointer
 * @return        character string
 */
char *resTargetToString(ResTarget  *target,
                        bool       useAliasIfPresent) {
    char *vars = palloc0(1024);
    if (target->name && useAliasIfPresent) {
        sprintf(eos(vars), "%s", target->name);
    } else {
        if (IsA(target->val, ColumnRef)) {
            ColumnRef *ref = (ColumnRef *)target->val;
            /*
                char *cri_name = palloc(1024);
                ListCell* cell;
                foreach(cell, ref->fields){
                    sprintf(eos(cri_name),"%s",valToString(lfirst(cell)));
                    if(cell->next){
                        sprintf(eos(cri_name),".");
                    }
                }
            */
            sprintf(eos(vars), "%s", valToString(lfirst(ref->fields->tail)));
        } else if (IsA(target->val, A_Const)) {
            A_Const *c = (A_Const *)target->val;
            sprintf(eos(vars), "%s", valToStringEscape(&c->val, 1));
        } else if (IsA(target->val, FuncCall)) {
            //TODO: VERY DIRTY -> FIX
            FuncCall *c       = (FuncCall *)target->val;
            ColumnRef *colref = lfirst(c->args->head);
            if (target->name){
                sprintf(eos(vars), "%s(%s) as %s", valToString(lfirst(c->funcname->head)), concatValues(colref->fields, "."), target->name);
            }
            else{
                sprintf(eos(vars), "%s(%s) as __%s%i", valToString(lfirst(c->funcname->head)), concatValues(colref->fields, "."), valToString(lfirst(c->funcname->head)), c->location);
            }
        } else {
            elog(ERROR, "Unkown node type:%s\n", nodeToString(target));
        }
    }
    return vars;
}

/** General query cstring operations **/

/**
 * Creates a String with alias given as id
 * @param  string  input character string
 * @param  aliasId alias id
 * @return         Value* String
 */
Value *makeStringWithAlias(char  *string,
                           int   aliasId) {
    char *newstring = palloc0(64);
    sprintf(newstring, "%s __ALIAS_%i", string, aliasId);
    return makeString(newstring);
}

/**
 * batched version of query q
 * @param  queryToBatch   the query q to batch
 * @param  parameterQuery the parameter table/query for the parameterized batched query
 * @return                batched query
 */
char *qb(char  *queryToBatch,
         char  *parameterQuery) {
    char *newQuery        = palloc0(1024);
    char *paramQueryLabel = "__S";
    char *baseTableLabel  = "P";
    char *parameterRefs   = concatValuesWithPrefix(resolveQueryRefsSkipNFull(parameterQuery,0), ",", paramQueryLabel);
    char *inputQueryRefs  = concatValues(resolveQueryRefsSkipNFull(queryToBatch,0), ",");
    sprintf(newQuery,       "SELECT %s, %s ", parameterRefs, inputQueryRefs);
    sprintf(eos(newQuery),  "FROM (%s) %s LEFT OUTER JOIN PART %s  ", parameterQuery, paramQueryLabel, baseTableLabel);
    sprintf(eos(newQuery),  "ON %s.s_size = %s.p_size  ", paramQueryLabel, baseTableLabel);
    sprintf(eos(newQuery),  "GROUP BY %s  ", concatValuesWithPrefix(resolveQueryRefs(parameterQuery), ",", paramQueryLabel));
    return newQuery;
}

/**
 * makes an sql projection
 * @param  baseQuery     input query
 * @param  newSelects    projected columns
 * @param  subQueryAlias alias for the subquery
 * @return               projected query as character string
 */
char *projectWithAlias(char  *baseQuery,
                       char  *newSelects,
                       char  *subQueryAlias) {
    char *query = palloc0(1024);
    sprintf(query, "SELECT %s FROM (%s) %s", newSelects, baseQuery, subQueryAlias);
    return query;
}

/**
 * makes an sql projection
 * @param  baseQuery     input query
 * @param  newSelects    projected columns
 * @return               projected query as character string
 */
char *project(char  *baseQuery,
              char  *newSelects) {
    char *query = palloc0(1024);
    sprintf(query, "SELECT %s FROM (%s)", newSelects, baseQuery);
    return query;
}

/**
 * makes a left outer join
 * @param  baseQuery   left input query
 * @param  secondQuery right input query
 * @param  params      parameters that will be compared
 * @param  aliasLeft   alias for the left query
 * @param  aliasRight  alias for the right query
 * @return             left outer join query
 */
char *leftOuterJoin(char  *baseQuery,
                    char  *secondQuery,
                    List  *params,
                    char  *aliasLeft,
                    char  *aliasRight) {
    char *query = palloc0(1024);
    sprintf(query, "(%s) %s LEFT OUTER JOIN (%s) %s ON %s ", baseQuery, aliasLeft, secondQuery, aliasRight, concatMultipleValues(params, params, " = ", " AND ", aliasLeft, aliasRight));
    return query;
}

/**
 * Resolves the referenced query columns and skips the first n results
 * @param  query the input query
 * @param  n     skip these n elements
 * @return       List with the referenced columns
 */
List *resolveQueryRefsSkipNFull(char  *query,
                                int   n) {
    SelectStmt *select  = ((SelectStmt *)lfirst(raw_parser(query)->head));
    List *list = NIL;
    int i = 0;
    ListCell *l;
    foreach(l, select->targetList) {
        i++;
        if (i <= n)
            continue;
        list = lappend(list, makeString(resTargetToString(lfirst(l),0)));
    }
    return list;
}

/**
 * Resolves the referenced query columns and skips the first n results
 * @param  query the input query
 * @param  n     skip these n elements
 * @return       List with the referenced columns
 */
List *resolveQueryRefsSkipN(char  *query,
                            int   n) {
    SelectStmt *select  = ((SelectStmt *)lfirst(raw_parser(query)->head));
    List *list = NIL;
    int i = 0;
    ListCell *l;
    foreach(l, select->targetList) {
        i++;
        if (i <= n)
            continue;
        list = lappend(list, makeString(resTargetToString(lfirst(l),1)));
    }
    return list;
}

/**
 * Resolves the referenced query columns
 * @param  query the input query
 * @return       List with the referenced columns
 */
List *resolveQueryRefs(char *query) {
    return resolveQueryRefsSkipN(query, 0);
}

/** PLpgSQL operations **/

/**
 * Creates a assignment statement
 * @param  varno variable number for the assignment
 * @param  query   query for the assignment
 * @return       the assignment statement
 */
PLpgSQL_stmt_assign *createQueryAssignment( int varno,
                                            char* query) {
    PLpgSQL_stmt_assign *newAssign = palloc(sizeof(PLpgSQL_stmt_assign));
    newAssign->cmd_type            = PLPGSQL_STMT_ASSIGN;
    newAssign->varno               = varno;
    newAssign->expr                = palloc0(sizeof(PLpgSQL_expr));
    newAssign->expr->query         = query;
    newAssign->expr->dtype         = PLPGSQL_DTYPE_EXPR;
    return newAssign;
}

/**
 * Creates a fors statement
 * @param  query the query the fors statements iterates
 * @param  rec   record
 * @param  row   row
 * @param  body  substatements
 * @param  func  surrounding function
 * @return       the fors statement
 */
PLpgSQL_stmt_fors *createFors(  char *query,
                                PLpgSQL_rec *rec,
                                PLpgSQL_row *row,
                                List *body,
                                PLpgSQL_function *func) {
    PLpgSQL_stmt_fors *newFors = palloc0(sizeof(PLpgSQL_stmt_fors));
    newFors->cmd_type          = PLPGSQL_STMT_FORS;
    newFors->query             = palloc0(sizeof(PLpgSQL_expr));
    newFors->query->dtype      = PLPGSQL_DTYPE_EXPR;
    newFors->query->query      = query;
    newFors->query->func       = func;
    newFors->rec               = rec;
    newFors->row               = row;
    newFors->body              = body;
    return newFors;
}

/**
 * replaces oldStmt with newStmt
 * @param oldStmt the old one
 * @param newStmt the new one
 * @param parents parents of the oldStmt
 */
void replaceStmt(PLpgSQL_stmt  *oldStmt,
                 PLpgSQL_stmt  *newStmt,
                 List          *parents) {
    ListCell *l;
    /* iterate over the statements */
    foreach(l, parents) {
        if (oldStmt == lfirst(l)) {
            lfirst(l) = newStmt;
            break;
        }
    }
}

/**
 * With matching variables to the query return the query column that matches one of those vars given as input
 * @param  varno input var
 * @param  vars  all vars matching the query columns
 * @param  nvars number of vars
 * @param  query the query
 * @return       column matching the variable with id varno
 */
Value *resolveTargetRefStrToCorrespVarno(   int  varno,
                                            int  *vars,
                                            int  nvars,
                                            char *query) {
    List *queryRefs = resolveQueryRefs(query);
    for (int i = 0; i < nvars; i++) {
        int curvar = vars[i];
        if (curvar == varno) {
            return linitial(queryRefs);
        }
        queryRefs = list_delete_first(queryRefs);
    }
    return NULL;
}
/**
 * Resolve a list of varnames to a given Bitmapset with ids
 * @param  bms_in Bitmapset with ids
 * @param  estate current state
 * @param  func   surrounding function
 * @return        list of varnames
 */
List *resolveBmsRefs(Bitmapset          *bms_in) {
    Bitmapset *bms = bms_copy(bms_in);
    List *list = NIL;
    while (!bms_is_empty(bms)) {
        int varno     = bms_first_member(bms);
        char *varname = varnumberToVarname(     varno,
                                                plpgsql_Datums);
        list          = lappend(list, varname);
    }
    return list;
}

/**
 * For i ∈ {1,...,m}, j ∈ {1,...,n} : c′i′ := cj if c′i = vj else c′i′ := c′i
 * @param  bms_in  input bitmapset
 * @param  vars    input variable ids
 * @param  nvars   number of variable ids
 * @param  query   matching query to variables
 * @param  estate  current state
 * @param  func    surrounding function
 * @param  aliases aliases for the columns
 * @return         List of column names that match the input variables or if not matches the variable name
 */
List *resolveQueryColumns(Bitmapset          *bms_in,
                          int                *vars,
                          int                nvars,
                          char               *query,
                          PLpgSQL_execstate  *estate,
                          PLpgSQL_function   *func,
                          bool               aliases) {
    Bitmapset *bms = bms_copy(bms_in);
    List *list = NIL;
    while (!bms_is_empty(bms)) {
        int varno  = bms_first_member(bms);
        Value *val = resolveTargetRefStrToCorrespVarno( varno,
                                                        vars,
                                                        nvars,
                                                        query);
        if (val == NULL) {
            if (aliases) {
                val = makeStringWithAlias(varnumberToVarname(varno, plpgsql_Datums), varno);
            } else {
                val = makeString(varnumberToVarname(varno, plpgsql_Datums));
            }
        }
        list = lappend(list, val);
    }
    return list;
}

/**
 * resolve query columns with record
 * @param  bms_in  input
 * @param  record  [description]
 * @param  query   matching query to variables
 * @param  estate  current state
 * @param  func    surrounding function
 * @param  aliases aliases for the columns
 * @return         List of column names that match the input variables or if not matches the variable name
 */
List *resolveQueryColumnsWithRecord(Bitmapset          *bms_in,
                                    PLpgSQL_rec *      record,
                                    char               *query,
                                    PLpgSQL_execstate  *estate,
                                    PLpgSQL_function   *func,
                                    bool               aliases) {
    Bitmapset *bms = bms_copy(bms_in);
    List *list = NIL;
    while (!bms_is_empty(bms)) {
        int varno = bms_first_member(bms);
        Value *val;
        /* if variable accesses member of the given record use the fieldname and for access to the underlying query*/
        if(plpgsql_Datums[varno]->dtype == PLPGSQL_DTYPE_RECFIELD &&
          ((PLpgSQL_recfield*)plpgsql_Datums[varno])->recparentno == record->dno){
            PLpgSQL_recfield* recfield = (PLpgSQL_recfield*)plpgsql_Datums[varno];
            val = makeString(recfield->fieldname);
        }
        else{
            if (aliases) {
                val = makeStringWithAlias(varnumberToVarname(varno, plpgsql_Datums), varno);
            } else {
                val = makeString(varnumberToVarname(    varno,
                                                        plpgsql_Datums));
            }
        }
        list = lappend(list, val);
    }
    return list;
}

/**
 * creates a record and adds it to the bottom of the current namespace chain
 * @param  label label for the record
 * @return       newly created and allocated record
 */
PLpgSQL_rec* createRecordAndAddToNamespace(char* label){
    PLpgSQL_rec* rec = plpgsql_build_record(label,0,0);
    PLpgSQL_nsitem* ns_cur = plpgsql_ns_top();
    while(ns_cur->prev){
        ns_cur = ns_cur->prev;
    }
    PLpgSQL_nsitem* nse = palloc(sizeof(PLpgSQL_nsitem) + strlen(rec->refname));
    nse->itemtype = PLPGSQL_NSTYPE_REC;
    nse->itemno = rec->dno;
    nse->prev = NULL;
    strcpy(nse->name, rec->refname);
    ns_cur->prev = nse;
    return rec;
}

/**
 * handleFors
 * @param fors    fors statement
 * @param parents parent statements
 * @param estate  current state
 * @param func    function
 */
// TODO: CREATE labels dynamically
void handleFors(PLpgSQL_stmt_fors *fors ,
                List               *parents,
                PLpgSQL_execstate  *estate,
                PLpgSQL_function   *func) {
    /* Get the first substatement */
    PLpgSQL_stmt *firstStmt = lfirst(fors->body->head);
    /* if the first substatement is PLPGSQL_STMT_EXECSQL continue */
    if (firstStmt && firstStmt->cmd_type == PLPGSQL_STMT_EXECSQL) {
        PLpgSQL_stmt_execsql *execsql = (PLpgSQL_stmt_execsql *)firstStmt;
        if (execsql->into) {
             PLpgSQL_expr *q               = execsql->sqlstmt;
             PLpgSQL_expr *qO              = fors->query;
             /*
             * Parameters of the EXECSQL into query q: c′1,...,c′m
             */
            Bitmapset *c_                  = getParametersOfQueryExpr( q,
                                                                       plpgsql_Datums,
                                                                       plpgsql_nDatums,
                                                                       func,
                                                                       estate);
             int m                         = bms_num_members(c_);
             /* we need to be in the function context */
             MemoryContext compile_tmp_cxt = MemoryContextSwitchTo(func_cxt);
             /* palloc space for a new query */
             char *qbquery                 = palloc0(1024);
             /*
             * The part πc′1′ ,...,c′m′ (qO) accesses the qO query and projects to the previous input parameters c′1,...,c′m of the assignment-EXECSQL in the FOR-loop.
             * If one of these parameters is a plsql- variable bound by the input FORS statement it is not available at this point.
             * So we have to replace it with the corresponding projected column of the query qO.
             */
            List *c_new;
            // TODO: Simplify !!!
            if(fors->row){
                c_new = resolveQueryColumns(  c_,
                                              fors->row->varnos,
                                              fors->row->nfields,
                                              qO->query,
                                              estate,
                                              func,
                                              1);
            }
            else{
                c_new = resolveQueryColumnsWithRecord(
                                                c_,
                                                fors->rec,
                                                qO->query,
                                                estate,
                                                func,
                                                1);
            }
            /* nummber of columns referenced by the input fors query */
            int n = c_new->length;

            sprintf(qbquery, "%s", projectWithAlias(qO->query,
                                                    concatValues( c_new,
                                                            ","), "_qO"));
            /*
             * The result of this step is that we now have a batched version of the different input parameters of query q.
             * This batched input parameters can now be passed to the batched version qb of query q.
             * When executing, the result will be a table with m + k columns where for every row the last k columns correspond
             * to the result of executing query q using the first m columns as input parameters.
             */
            sprintf(qbquery, "%s", qb(q->query, qbquery));
            /**
            * c1,...,ck
            */
            List *c   = resolveQueryRefs(qO->query);
            /**
            * c′′1,...,c′′k
            */
            List *c__ = resolveQueryRefsSkipN(qbquery, m);
            /**
             * Project to the columns c1,...,cn,c′′1,...,c′′k
             */
            sprintf(qbquery, "SELECT %s,%s FROM %s", concatValuesWithPrefix(c, ",", "__c"),
                    concatValuesWithPrefix(c__, ",", "__c__"),
                    leftOuterJoin(
                        qO->query,
                        qbquery,
                        c_new,
                        "__c",//TODO: fix problems with outer scope. 
                        "__c__"));
            PLpgSQL_rec* rec = fors->rec;
            List* body       = NIL;
            /* Rule 1a-1 or 1a-3 (Rows-FORS statement) */
            if(fors->row){
                /* create a record because we do not have one */
                rec = createRecordAndAddToNamespace("__rec");

                /* resolve the column refs of the new FORS-LOOP */
                List *cRes = resolveQueryRefs(qbquery);
                /* iterate the FORS input columns and create assignment statements for the input FORS statment columns */
                for (int i = 0; i < n; i++) {
                    char* newQuery                     = palloc0(64);
                    /* query to retrieve the current fors input column from the record */
                    sprintf(newQuery,"SELECT %s.%s",rec->refname, valToString(list_nth(cRes,i)));
                    /* Create a corresponding recordfield that will be referenced by the assignment */
                    //TODO: put in function
                    PLpgSQL_recfield* recfield         = palloc(sizeof(PLpgSQL_recfield));
                    recfield->dtype                    = PLPGSQL_DTYPE_RECFIELD;
                    recfield->fieldname                = valToString(list_nth(cRes,i));
                    recfield->recparentno              = rec->dno;
                    plpgsql_adddatum((PLpgSQL_datum *)recfield);
                    /* Create asssignment that puts the record column in the correct plpgsql variable vi */
                    PLpgSQL_stmt_assign *newAssignment = createQueryAssignment( fors->row->varnos[i],
                                                                                newQuery);
                    /* use namespace of the execsql query */
                    newAssignment->expr->ns            = execsql->sqlstmt->ns;
                    /* append it to the body of the new FORS-loop */
                    body                               = lappend(body,newAssignment);
                }
            }
            /* resolve the column refs of the new FORS-LOOP, skip n elements to get the m columns referenced by the input execsql statement */
            List *cRes = resolveQueryRefsSkipN(qbquery,n);
            for (int i = 0; i < cRes->length; i++) {
                char* newQuery             = palloc0(64);
                /* query to retrieve the current execsql input column from the record */
                sprintf(newQuery,"SELECT %s.%s",rec->refname, valToString(list_nth(cRes,i)));
                /* Create a corresponding recordfield that will be referenced by the assignment */
                //TODO: put in function
                PLpgSQL_recfield* recfield = palloc(sizeof(PLpgSQL_recfield));
                recfield->dtype            = PLPGSQL_DTYPE_RECFIELD;
                recfield->fieldname        = valToString(list_nth(cRes,i));
                recfield->recparentno      = rec->dno;
                plpgsql_adddatum((PLpgSQL_datum *)recfield);
                /* If we have row-variables in the execsql statement create a asssignment that puts the record column in the correct plpgsql variable v''i */
                if(execsql->row){
                    PLpgSQL_stmt_assign *newAssignment = createQueryAssignment( execsql->row->varnos[i],
                                                                                newQuery);
                    /* use namespace of the execsql query */
                    newAssignment->expr->ns            = execsql->sqlstmt->ns;
                    /* append it to the body of the new FORS-loop */
                    body                               = lappend(body,newAssignment);
                }
            }
            /* If we have  a record-variable in the execsql statement create a asssignment that copys the new record to this one */
            if(execsql->rec){
                char* newQuery             = palloc0(64);
                /* select the new record */
                sprintf(newQuery,"SELECT %s",rec->refname);
                /* assign the new record to the execsql recoord */
                PLpgSQL_stmt_assign *newAssignment = createQueryAssignment(execsql->rec->dno, newQuery);
                /* use namespace of the execsql query */
                newAssignment->expr->ns            = execsql->sqlstmt->ns;
                /* append it to the body of the new FORS-loop */
                body                               = lappend(body,newAssignment);
            }
            /* copy the following body of the input FORS-loop */
            body                       = list_concat(body,list_copy_tail(fors->body, 1));

            /* assemble the new FORS-loop */
            PLpgSQL_stmt_fors *newFors = createFors(qbquery,
                                                    rec,
                                                    NULL,
                                                    body,
                                                    func);
            /**
             * Now the new FORS loop is correctly assembled and can replace the input FORS loop and the single-row assignment-EXECSQL statement.
             */
            replaceStmt((PLpgSQL_stmt *)fors, (PLpgSQL_stmt *)newFors, parents);
            /* Switch back to the tmp cxt */
            MemoryContextSwitchTo(compile_tmp_cxt);
        }
    }
}

/**
 * starts the analyse process that will apply the Rules
 * @param estate current state
 * @param func   surrounding function
 * @param cxt    Function MemoryContext
 */
void analyse(PLpgSQL_execstate  *estate,
             PLpgSQL_function   *func,
             MemoryContext      cxt) {


    /* convert the statements to an flow-graph */
    igraph_t* igraph = createFlowGraph(plpgsql_Datums,plpgsql_nDatums,func,estate);
    /* perform depenence analysis operations on igraph */
    addProgramDependenceEdges(igraph);

    func_cxt = cxt;
    List *statements = func->action->body;
    ListCell *l;
    /* iterate over the statements */
    foreach(l, list_copy(statements)) {
        /* Get the statement */
        PLpgSQL_stmt *stmt = lfirst(l);
        switch (stmt->cmd_type) {
            case PLPGSQL_STMT_FORS: {
                handleFors((PLpgSQL_stmt_fors  *)  stmt,
                           statements,
                           estate,
                           func);
                break;
            }
        }
    }
}
