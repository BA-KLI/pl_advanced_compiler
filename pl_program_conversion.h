#include "plpgsql.h"
#include "nodes/pg_list.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>


#define MAXDOTFILESIZE 4096

#define eos(s) ((s)+strlen(s))




/**********************************************************************
 * Function declarations
 **********************************************************************/


/* ----------
 * Functions in pl_program_conversion.c
 * ----------
 */
void analyse(PLpgSQL_execstate *estate, PLpgSQL_function *func, MemoryContext cxt) ;

