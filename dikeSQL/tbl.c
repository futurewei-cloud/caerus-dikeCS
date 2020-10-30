
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h> 
#include <unistd.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

/* This is a copy for sqlite3.c file */
#define SQLITE_AFF_NONE     0x40  /* '@' */
#define SQLITE_AFF_BLOB     0x41  /* 'A' */
#define SQLITE_AFF_TEXT     0x42  /* 'B' */
#define SQLITE_AFF_NUMERIC  0x43  /* 'C' */
#define SQLITE_AFF_INTEGER  0x44  /* 'D' */
#define SQLITE_AFF_REAL     0x45  /* 'E' */

/*
** A macro to hint to the compiler that a function should not be
** inlined.
*/
#if defined(__GNUC__)
#  define CSV_NOINLINE  __attribute__((noinline))
#  define CSV_ALWAYS_INLINE  __attribute__((always_inline)) inline
#  define CSV_LIKELY(x)      __builtin_expect(!!(x), 1)
#  define CSV_UNLIKELY(x)    __builtin_expect(!!(x), 0)
#elif defined(_MSC_VER) && _MSC_VER>=1310
#  define CSV_NOINLINE  __declspec(noinline)
#else
#  define CSV_NOINLINE
#endif


/* Max size of the error message in a CsvReader */
#define CSV_MXERR 200

/* Size of the CsvReader input buffer */
//#define CSV_INBUFSZ 1024
#define CSV_INBUFSZ (4<<20)

static const char TBL_FDELIM = '|';

/* A context object used when read a CSV file. */
typedef struct CsvReader CsvReader;
struct CsvReader {
  FILE *in;              /* Read the CSV text from this input stream */
  char *z;               /* Accumulated text for a field */
  int n;                 /* Number of bytes in z */
  int nAlloc;            /* Space allocated for z[] */
  int nLine;             /* Current line number */
  //int bNotFirst;       /* True if prior text has been seen */
  int cTerm;             /* Character that terminated the most recent field */
  size_t iIn;            /* Next unread character in the input buffer */
  size_t nIn;            /* Number of characters in the input buffer */
  char *zIn;             /* The input buffer */
  char *zStreamBuf[2];   /* Two temporary buffers for async input stream */
  char * zStream;        /* Input thread will read file here */
  int  nStream;        /* Number of characters in the nStream buffer */
  pthread_t thread;      /* Input stream thread id */
  sem_t sem;             /* Input stream syncronization semaphore */
  pthread_mutex_t lock; 

  char zErr[CSV_MXERR];  /* Error message */
};

/* Report an error on a CsvReader */
static void csv_errmsg(CsvReader *p, const char *zFormat, ...){
  va_list ap;
  va_start(ap, zFormat);
  sqlite3_vsnprintf(CSV_MXERR, p->zErr, zFormat, ap);
  va_end(ap);
}

/* Initialize a CsvReader object */
static void csv_reader_init(CsvReader *p){ 
  p->in = 0;
  p->z = 0;
  p->n = 0;
  p->nAlloc = 0;
  p->nLine = 0;
  //p->bNotFirst = 0;
  p->nIn = 0;
  p->zIn = 0;
  p->zErr[0] = 0;
  p->zStreamBuf[0] = 0;
  p->zStreamBuf[1] = 0;
  p->nStream = -1;
}

/* Close and reset a CsvReader object */
static void csv_reader_reset(CsvReader *p){
  if( p->in ){
    sem_post(&p->sem);
    pthread_mutex_lock(&p->lock);
    fclose(p->in);
    p->in = 0;
    pthread_mutex_unlock(&p->lock);
    sem_post(&p->sem);
    //sqlite3_free(p->zIn);
    pthread_join(p->thread, NULL);
    free(p->zStreamBuf[0]);
    free(p->zStreamBuf[1]);
  }
  sqlite3_free(p->z);
  csv_reader_init(p);
}

static void *tbl_stream_thread(void *arg)
{
  CsvReader *p = (CsvReader *)arg;
  
  while(1) {
    sem_wait(&p->sem);
    pthread_mutex_lock(&p->lock);
    if( p->in==0 ){
      pthread_mutex_unlock(&p->lock);
      return 0;
    }
    p->nStream = fread(p->zStream, 1, CSV_INBUFSZ, p->in);
    if(p->nStream < CSV_INBUFSZ){
      p->zStream[p->nStream] = EOF;
    }
    if( p->nStream==0 ){
      pthread_mutex_unlock(&p->lock);
      //p->zStream[0] = EOF;
      return 0;
    }
    pthread_mutex_unlock(&p->lock);
  }

  return 0;
}

/* Open the file associated with a CsvReader
** Return the number of errors.
*/
static int csv_reader_open(
  CsvReader *p,               /* The reader to open */
  const char *zFilename,      /* Read from this filename */
  const char *zData           /*  ... or use this data */
){
  if( zFilename ){
    /*
    p->zIn = sqlite3_malloc( CSV_INBUFSZ );
    if( p->zIn==0 ){
      csv_errmsg(p, "out of memory");
      return 1;
    }
    */
    p->zStreamBuf[0] = malloc(CSV_INBUFSZ);
    p->zStreamBuf[1] = malloc(CSV_INBUFSZ);
    if(p->zStreamBuf[0]==0 || p->zStreamBuf[1]==0){
      csv_errmsg(p, "out of memory");
      return 1;
    }

    p->in = fopen(zFilename, "rb");
    if( p->in==0 ){
      //sqlite3_free(p->zIn);
      free(p->zStreamBuf[0]);
      free(p->zStreamBuf[1]);
      csv_reader_reset(p);
      csv_errmsg(p, "cannot open '%s' for reading", zFilename);
      return 1;
    }
    p->zStream = p->zStreamBuf[0];
    p->nStream = -1;
    sem_init(&p->sem,0,1);
    pthread_mutex_init(&p->lock, NULL);
    pthread_create(&p->thread,NULL,tbl_stream_thread,p);

  }else{
    assert( p->in==0 );
    p->zIn = (char*)zData;
    p->nIn = strlen(zData);
  }
  return 0;
}


/* The input buffer has overflowed.  Refill the input buffer, then
** return the next character
*/
static CSV_NOINLINE int csv_getc_refill(CsvReader *p){
  size_t got;

  assert( p->iIn>=p->nIn );  /* Only called on an empty input buffer */
  assert( p->in!=0 );        /* Only called if reading froma file */

  while(1){
    pthread_mutex_lock(&p->lock);    
    if(p->nStream == -1){ /* Unlikely timing race */
      pthread_mutex_unlock(&p->lock);
      usleep(100);
    } else{
      break;
    }
  }

  got = p->nStream;

  if( got==0 ) {
    pthread_mutex_unlock(&p->lock);
    return EOF;
  }

  p->zIn = p->zStream;
  if(p->zStream == p->zStreamBuf[0]){
    p->zStream = p->zStreamBuf[1];
  } else {
    p->zStream = p->zStreamBuf[0];
  }
  p->nStream = -1;
  pthread_mutex_unlock(&p->lock);

  sem_post(&p->sem);

  //got = fread(p->zIn, 1, CSV_INBUFSZ, p->in);

  //if( got==0 ) return EOF;
  p->nIn = got;
  p->iIn = 1;
  return p->zIn[0];  
}

/* Return the next character of input.  Return EOF at end of input. */
static CSV_ALWAYS_INLINE int csv_getc(CsvReader *p){
  if(CSV_UNLIKELY( p->iIn >= p->nIn )){
    if( p->in!=0 ) return csv_getc_refill(p);
    return EOF;
  }
  return ((unsigned char*)p->zIn)[p->iIn++];
}

/* Increase the size of p->z and append character c to the end. 
** Return 0 on success and non-zero if there is an OOM error */
static CSV_NOINLINE int csv_resize_and_append(CsvReader *p, char c){
  char *zNew;
  int nNew = p->nAlloc*2 + 100;
  zNew = sqlite3_realloc64(p->z, nNew);
  if( CSV_LIKELY(zNew) ){
    p->z = zNew;
    p->nAlloc = nNew;
    p->z[p->n++] = c;
    return 0;
  }else{
    csv_errmsg(p, "out of memory");
    return 1;
  }
}

/* Append a single character to the CsvReader.z[] array.
** Return 0 on success and non-zero if there is an OOM error */
static CSV_ALWAYS_INLINE int csv_append(CsvReader *p, char c){
  if( CSV_UNLIKELY(p->n>=p->nAlloc-1)) return csv_resize_and_append(p, c);
  p->z[p->n++] = c;
  return 0;
}

/* Read a single field of CSV text.  Compatible with rfc4180 and extended
** with the option of having a separator other than ",".
**
**   +  Input comes from p->in.
**   +  Store results in p->z of length p->n.  Space to hold p->z comes
**      from sqlite3_malloc64().
**   +  Keep track of the line number in p->nLine.
**   +  Store the character that terminates the field in p->cTerm.  Store
**      EOF on end-of-file.
**
** Return 0 at EOF or on OOM.  On EOF, the p->cTerm character will have
** been set to EOF.
*/
static char *tbl_read_one_field(CsvReader *p, int delim){
  int c;
  p->n = 0;
  c = csv_getc(p);
  if( CSV_UNLIKELY(c==EOF) ){
    p->cTerm = EOF;
    return 0;
  }
  if( CSV_UNLIKELY(c=='"') ){
    int pc, ppc;
    int startLine = p->nLine;
    pc = ppc = 0;
    while( 1 ){
      c = csv_getc(p);
      if( c<='"' || pc=='"' ){
        if( c=='\n' ) p->nLine++;
        if( c=='"' ){
          if( pc=='"' ){
            pc = 0;
            continue;
          }
        }
        if( (c==delim && pc=='"')
         || (c=='\n' && pc=='"')
         || (c=='\n' && pc=='\r' && ppc=='"')
         || (c==EOF && pc=='"')
        ){
          do{ p->n--; }while( p->z[p->n]!='"' );
          p->cTerm = (char)c;
          break;
        }
        if( pc=='"' && c!='\r' ){
          csv_errmsg(p, "line %d: unescaped %c character", p->nLine, '"');
          break;
        }
        if( c==EOF ){
          csv_errmsg(p, "line %d: unterminated %c-quoted field\n",
                     startLine, '"');
          p->cTerm = (char)c;
          break;
        }
      }
      if( csv_append(p, (char)c) ) return 0;
      ppc = pc;
      pc = c;
    }
  } else {
    while(c!=0 && c!=delim && c!='\n' && c!=EOF){
      if( csv_append(p, (char)c) ) return 0;
      c = csv_getc(p);
    }
    if( c=='\n' ){
      p->nLine++;
      if( p->n>0 && p->z[p->n-1]=='\r' ) p->n--;
    }
    p->cTerm = (char)c;
  }
  if( p->z ) p->z[p->n] = 0;
  //p->bNotFirst = 1;
  return p->z;
}


/* Forward references to the various virtual table methods implemented
** in this file. */
static int tbltabCreate(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int tbltabConnect(sqlite3*, void*, int, const char*const*, 
                           sqlite3_vtab**,char**);
static int tbltabBestIndex(sqlite3_vtab*,sqlite3_index_info*);
static int tbltabDisconnect(sqlite3_vtab*);
static int tbltabOpen(sqlite3_vtab*, sqlite3_vtab_cursor**);
static int tbltabClose(sqlite3_vtab_cursor*);
static int tbltabFilter(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                          int argc, sqlite3_value **argv);
static int tbltabNext(sqlite3_vtab_cursor*);
static int tbltabEof(sqlite3_vtab_cursor*);
static int tbltabColumn(sqlite3_vtab_cursor*,sqlite3_context*,int);
static int tbltabRowid(sqlite3_vtab_cursor*,sqlite3_int64*);

/* An instance of the CSV virtual table */
typedef struct CsvTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  char *zFilename;                /* Name of the CSV file */
  char *zData;                    /* Raw CSV data in lieu of zFilename */
  long iStart;                    /* Offset to start of data in zFilename */
  int nCol;                       /* Number of columns in the CSV file */
  unsigned int tstFlags;          /* Bit values used for testing */
  unsigned char cTypes[64];       /* Column affinity */
} CsvTable;

/* Allowed values for tstFlags */
#define CSVTEST_FIDX  0x0001      /* Pretend that constrained searchs cost less*/

/* A cursor for the CSV virtual table */
typedef struct CsvCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  CsvReader rdr;                  /* The CsvReader object */
  char **azVal;                   /* Value of the current row */
  int *aLen;                      /* Length of each entry */
  char **azPtr;                    /* Deliminator indices to reader array */
  sqlite3_int64 iRowid;           /* The current rowid.  Negative for EOF */
} CsvCursor;

/* Transfer error message text from a reader into a CsvTable */
static void csv_xfer_error(CsvTable *pTab, CsvReader *pRdr){
  sqlite3_free(pTab->base.zErrMsg);
  pTab->base.zErrMsg = sqlite3_mprintf("%s", pRdr->zErr);
}

/*
** This method is the destructor fo a CsvTable object.
*/
static int tbltabDisconnect(sqlite3_vtab *pVtab){
  CsvTable *p = (CsvTable*)pVtab;
  sqlite3_free(p->zFilename);
  sqlite3_free(p->zData);
  sqlite3_free(p);
  return SQLITE_OK;
}

/* Skip leading whitespace.  Return a pointer to the first non-whitespace
** character, or to the zero terminator if the string has only whitespace */
static const char *csv_skip_whitespace(const char *z){
  while( isspace((unsigned char)z[0]) ) z++;
  return z;
}

/* Remove trailing whitespace from the end of string z[] */
static void csv_trim_whitespace(char *z){
  size_t n = strlen(z);
  while( n>0 && isspace((unsigned char)z[n]) ) n--;
  z[n] = 0;
}

/* Dequote the string */
static void csv_dequote(char *z){
  int j;
  char cQuote = z[0];
  size_t i, n;

  if( cQuote!='\'' && cQuote!='"' ) return;
  n = strlen(z);
  if( n<2 || z[n-1]!=z[0] ) return;
  for(i=1, j=0; i<n-1; i++){
    if( z[i]==cQuote && z[i+1]==cQuote ) i++;
    z[j++] = z[i];
  }
  z[j] = 0;
}

/* Check to see if the string is of the form:  "TAG = VALUE" with optional
** whitespace before and around tokens.  If it is, return a pointer to the
** first character of VALUE.  If it is not, return NULL.
*/
static const char *csv_parameter(const char *zTag, int nTag, const char *z){
  z = csv_skip_whitespace(z);
  if( strncmp(zTag, z, nTag)!=0 ) return 0;
  z = csv_skip_whitespace(z+nTag);
  if( z[0]!='=' ) return 0;
  return csv_skip_whitespace(z+1);
}

/* Decode a parameter that requires a dequoted string.
**
** Return 1 if the parameter is seen, or 0 if not.  1 is returned
** even if there is an error.  If an error occurs, then an error message
** is left in p->zErr.  If there are no errors, p->zErr[0]==0.
*/
static int csv_string_parameter(
  CsvReader *p,            /* Leave the error message here, if there is one */
  const char *zParam,      /* Parameter we are checking for */
  const char *zArg,        /* Raw text of the virtual table argment */
  char **pzVal             /* Write the dequoted string value here */
){
  const char *zValue;
  zValue = csv_parameter(zParam,(int)strlen(zParam),zArg);
  if( zValue==0 ) return 0;
  p->zErr[0] = 0;
  if( *pzVal ){
    csv_errmsg(p, "more than one '%s' parameter", zParam);
    return 1;
  }
  *pzVal = sqlite3_mprintf("%s", zValue);
  if( *pzVal==0 ){
    csv_errmsg(p, "out of memory");
    return 1;
  }
  csv_trim_whitespace(*pzVal);
  csv_dequote(*pzVal);
  return 1;
}


/* Return 0 if the argument is false and 1 if it is true.  Return -1 if
** we cannot really tell.
*/
static int csv_boolean(const char *z){
  if( sqlite3_stricmp("yes",z)==0
   || sqlite3_stricmp("on",z)==0
   || sqlite3_stricmp("true",z)==0
   || (z[0]=='1' && z[1]==0)
  ){
    return 1;
  }
  if( sqlite3_stricmp("no",z)==0
   || sqlite3_stricmp("off",z)==0
   || sqlite3_stricmp("false",z)==0
   || (z[0]=='0' && z[1]==0)
  ){
    return 0;
  }
  return -1;
}

/* Check to see if the string is of the form:  "TAG = BOOLEAN" or just "TAG".
** If it is, set *pValue to be the value of the boolean ("true" if there is
** not "= BOOLEAN" component) and return non-zero.  If the input string
** does not begin with TAG, return zero.
*/
static int csv_boolean_parameter(
  const char *zTag,       /* Tag we are looking for */
  int nTag,               /* Size of the tag in bytes */
  const char *z,          /* Input parameter */
  int *pValue             /* Write boolean value here */
){
  int b;
  z = csv_skip_whitespace(z);
  if( strncmp(zTag, z, nTag)!=0 ) return 0;
  z = csv_skip_whitespace(z + nTag);
  if( z[0]==0 ){
    *pValue = 1;
    return 1;
  }
  if( z[0]!='=' ) return 0;
  z = csv_skip_whitespace(z+1);
  b = csv_boolean(z);
  if( b>=0 ){
    *pValue = b;
    return 1;
  }
  return 0;
}

static int tbl_parse_until(char * str, const char *expr, int * pos)
{
  int p = *pos;
  char * debug = &str[p];
  int expr_len = strlen(expr);
  int i;

  while(str[p] != 0) {
    for(i = 0; i < expr_len; i++){
      if(str[p]==expr[i]){
        *pos = p + 1; /* We returning "interesting" position */
        return SQLITE_OK;  
      }
    }
    p++;
  }
  
  return SQLITE_ERROR;
}

static int tbl_parse_skip(char * str, int c, int * pos)
{
  int p = *pos;
  char * debug = &str[p];

  while(str[p] != 0) {
    if(str[p]!=c) {
      *pos = p;
      return SQLITE_OK;
    }
    p++;
  }

  return SQLITE_ERROR;
}

static int tbl_parse_schema(char * schema, CsvTable *pNew)
{
  int pos = 0;
  int len = strlen(schema);
  int col = 0; /* column counter */
  
  /* parse until opening bracket */
  if(tbl_parse_until(schema, "(", &pos)){
    return SQLITE_ERROR;
  }

  while(schema[pos-1] != ')') {
    if(tbl_parse_skip(schema, ' ', &pos)){
      return SQLITE_ERROR;
    }

    if(schema[pos]=='"'){ /* If quote , parse until end of it */
      pos++;
      if(tbl_parse_until(schema, "\"", &pos)){
        return SQLITE_ERROR;
      }
     } else { /* parse until space */
      if(tbl_parse_skip(schema, ' ', &pos)){
        return SQLITE_ERROR;
      }
    }

    if(tbl_parse_until(schema, "), ", &pos)){
      return SQLITE_ERROR;
    }
    
    /* if we found ',' or '\n' - we do not have data type */
    if(schema[pos-1]==',' || schema[pos-1]==')') {
      pNew->cTypes[col] = SQLITE_AFF_TEXT;
      col++;
      continue;
    }

    if(tbl_parse_skip(schema, ' ', &pos)){
      return SQLITE_ERROR;
    }

    /* Main logic */
    switch(schema[pos]) {
      case 'N':
        if(schema[pos+1]=='U'){
          pNew->cTypes[col] = SQLITE_AFF_NUMERIC;
        } else{
          pNew->cTypes[col] = SQLITE_AFF_NONE;
        }
        break;
      case 'B':
        pNew->cTypes[col] = SQLITE_AFF_BLOB;
        break;
      case 'T':
        pNew->cTypes[col] = SQLITE_AFF_TEXT;
        break;
      case 'I':
        pNew->cTypes[col] = SQLITE_AFF_INTEGER;
        break;
      case 'R':
        pNew->cTypes[col] = SQLITE_AFF_REAL;
        break;
      default:
        pNew->cTypes[col] = SQLITE_AFF_TEXT;
        break;                        
    }

    if(tbl_parse_until(schema, ",)" , &pos)){
      return SQLITE_ERROR;
    }

    col++;
  } 

  pNew->nCol = col;  

  return SQLITE_OK;
}

/*
** Parameters:
**    filename=FILENAME          Name of file containing CSV content
**    data=TEXT                  Direct CSV content.
**    schema=SCHEMA              Alternative CSV schema.
**    header=YES|NO              First row of CSV defines the names of
**                               columns if "yes".  Default "no".
**    columns=N                  Assume the CSV file contains N columns.
**
**    fielddelim=DELIM           Single character field delimiter
**
** Only available if compiled with SQLITE_TEST:
**    
**    testflags=N                Bitmask of test flags.  Optional
**
** If schema= is omitted, then the columns are named "c0", "c1", "c2",
** and so forth.  If columns=N is omitted, then the file is opened and
** the number of columns in the first row is counted to determine the
** column count.  If header=YES, then the first row is skipped.
*/
static int tbltabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  CsvTable *pNew = 0;        /* The CsvTable object to construct */
  int bHeader = -1;          /* header= flags.  -1 means not seen yet */
  int rc = SQLITE_OK;        /* Result code from this routine */
  int i, j;                  /* Loop counters */  
  
#ifdef SQLITE_TEST
  int tstFlags = 0;          /* Value for testflags=N parameter */
#endif
  int b;                     /* Value of a boolean parameter */
  int nCol = -99;            /* Value of the columns= parameter */
  CsvReader sRdr;            /* A CSV file reader used to store an error
                             ** message and/or to count the number of columns */
  static const char *azParam[] = {
     "filename", "data", "schema", 
  };
  char *azPValue[3];         /* Parameter values */
# define CSV_FILENAME (azPValue[0])
# define CSV_DATA     (azPValue[1])
# define CSV_SCHEMA   (azPValue[2])

  assert( sizeof(azPValue)==sizeof(azParam) );
  memset(&sRdr, 0, sizeof(sRdr));
  memset(azPValue, 0, sizeof(azPValue));
  for(i=3; i<argc; i++){
    const char *z = argv[i];
    const char *zValue;
    for(j=0; j<sizeof(azParam)/sizeof(azParam[0]); j++){
      if( csv_string_parameter(&sRdr, azParam[j], z, &azPValue[j]) ) break;
    }
    if( j<sizeof(azParam)/sizeof(azParam[0]) ){
      if( sRdr.zErr[0] ) goto tbltab_connect_error;
    }else
    if( csv_boolean_parameter("header",6,z,&b) ){
      if( bHeader>=0 ){
        csv_errmsg(&sRdr, "more than one 'header' parameter");
        goto tbltab_connect_error;
      }
      bHeader = b;
    }else
#ifdef SQLITE_TEST
    if( (zValue = csv_parameter("testflags",9,z))!=0 ){
      tstFlags = (unsigned int)atoi(zValue);
    }else
#endif
    if( (zValue = csv_parameter("columns",7,z))!=0 ){
      if( nCol>0 ){
        csv_errmsg(&sRdr, "more than one 'columns' parameter");
        goto tbltab_connect_error;
      }
      nCol = atoi(zValue);
      if( nCol<=0 ){
        csv_errmsg(&sRdr, "column= value must be positive");
        goto tbltab_connect_error;
      }
    }else
    {
      csv_errmsg(&sRdr, "bad parameter: '%s'", z);
      goto tbltab_connect_error;
    }
  }
  if( (CSV_FILENAME==0)==(CSV_DATA==0) ){
    csv_errmsg(&sRdr, "must specify either filename= or data= but not both");
    goto tbltab_connect_error;
  }

  if( CSV_SCHEMA==0 ){
    csv_errmsg(&sRdr, "must specify schema=");
    goto tbltab_connect_error;
  }
/*
  if( (nCol<=0 || bHeader==1)
   && csv_reader_open(&sRdr, CSV_FILENAME, CSV_DATA)
  ){
    goto tbltab_connect_error;
  }
*/
  pNew = sqlite3_malloc( sizeof(*pNew) );
  *ppVtab = (sqlite3_vtab*)pNew;
  if( pNew==0 ) goto tbltab_connect_oom;
  memset(pNew, 0, sizeof(*pNew));
  
  rc = tbl_parse_schema(CSV_SCHEMA, pNew);
  if( rc ){
    csv_errmsg(&sRdr, "bad schema: '%s' ", CSV_SCHEMA);
    goto tbltab_connect_error;
  }

  // We have nothing to read here
  //csv_reader_open(&sRdr, CSV_FILENAME, CSV_DATA);

  pNew->zFilename = CSV_FILENAME;  CSV_FILENAME = 0;
  pNew->zData = CSV_DATA;          CSV_DATA = 0;
  
  rc = sqlite3_declare_vtab(db, CSV_SCHEMA);
  if( rc ){
    csv_errmsg(&sRdr, "bad schema: '%s' - %s", CSV_SCHEMA, sqlite3_errmsg(db));
    goto tbltab_connect_error;
  }

  for(i=0; i<sizeof(azPValue)/sizeof(azPValue[0]); i++){
    sqlite3_free(azPValue[i]);
  }
  /* Rationale for DIRECTONLY:
  ** An attacker who controls a database schema could use this vtab
  ** to exfiltrate sensitive data from other files in the filesystem.
  ** And, recommended practice is to put all CSV virtual tables in the
  ** TEMP namespace, so they should still be usable from within TEMP
  ** views, so there shouldn't be a serious loss of functionality by
  ** prohibiting the use of this vtab from persistent triggers and views.
  */
  sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);

  return SQLITE_OK;

tbltab_connect_oom:
  rc = SQLITE_NOMEM;
  csv_errmsg(&sRdr, "out of memory");

tbltab_connect_error:
  if( pNew ) tbltabDisconnect(&pNew->base);
  for(i=0; i<sizeof(azPValue)/sizeof(azPValue[0]); i++){
    sqlite3_free(azPValue[i]);
  }
  if( sRdr.zErr[0] ){
    sqlite3_free(*pzErr);
    *pzErr = sqlite3_mprintf("%s", sRdr.zErr);
  }
  csv_reader_reset(&sRdr);
  if( rc==SQLITE_OK ) rc = SQLITE_ERROR;
  return rc;
}

/*
** Reset the current row content held by a CsvCursor.
*/
static void tbltabCursorRowReset(CsvCursor *pCur){
  CsvTable *pTab = (CsvTable*)pCur->base.pVtab;
  int i;
  for(i=0; i<pTab->nCol; i++){
    sqlite3_free(pCur->azVal[i]);
    pCur->azVal[i] = 0;
    pCur->aLen[i] = 0;
    pCur->azPtr[i] = 0;
  }
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int tbltabCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
 return tbltabConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Destructor for a CsvCursor.
*/
static int tbltabClose(sqlite3_vtab_cursor *cur){
  CsvCursor *pCur = (CsvCursor*)cur;
  tbltabCursorRowReset(pCur);
  csv_reader_reset(&pCur->rdr);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Constructor for a new CsvTable cursor object.
*/
static int tbltabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  CsvTable *pTab = (CsvTable*)p;
  CsvCursor *pCur;
  size_t nByte;
  nByte = sizeof(*pCur) + (sizeof(char*)+sizeof(int)+sizeof(char*))*pTab->nCol;
  pCur = sqlite3_malloc64( nByte );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, nByte);
  pCur->azVal = (char**)&pCur[1];
  pCur->aLen = (int*)&pCur->azVal[pTab->nCol];
  pCur->azPtr = (char**)&pCur->aLen[pTab->nCol];
  *ppCursor = &pCur->base;
  if( csv_reader_open(&pCur->rdr, pTab->zFilename, pTab->zData) ){
    csv_xfer_error(pTab, &pCur->rdr);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int tbl_get_field_indecies(CsvCursor *pCur, int nCol, int delim)
{
  int c;
  int i = 0;
  CsvReader *p = &pCur->rdr;
  char * azPtr;
  char * buf;
  char * end;
  int iIn = 0;

  if(CSV_UNLIKELY(p->nIn == 0)) {
    csv_getc_refill(p);
    buf = &p->zIn[0];
  } else {
    buf = &p->zIn[p->iIn];
    iIn++;
  }
    
  end = &p->zIn[p->nIn-1];
  do {
    //c = csv_getc(p);
    azPtr = buf;
    c = *buf;
    buf++;
    iIn++;
    if(buf >= end){ return SQLITE_IOERR; }

    if( CSV_UNLIKELY(c==EOF || c=='"') ){
        return SQLITE_IOERR;
    }

    while(c!=delim && c!='\n' && c!='"' && c!=EOF){      
      //c = csv_getc(p);
      //c = ((unsigned char*)p->zIn)[iIn-1];
      c = *buf;
      buf++;
      iIn++;
      //if(iIn >= nIn){ return SQLITE_IOERR; }
      if(buf >= end){ return SQLITE_IOERR; }
    }
    if( c==delim ){
      *(buf - 1) = 0;
      pCur->azPtr[i] = azPtr;
      azPtr = buf;
      i++;
    } else {
        return SQLITE_IOERR;
    }
  } while(i<nCol);

  if(*buf=='\n'){
    p->iIn += iIn;
  }

  return SQLITE_OK;
}

/*
** Advance a CsvCursor to its next row of input.
** Set the EOF marker if we reach the end of input.
*/
static int tbltabNext(sqlite3_vtab_cursor *cur){
  CsvCursor *pCur = (CsvCursor*)cur;
  CsvTable *pTab = (CsvTable*)cur->pVtab;
  int i = 0;
  char *z;
  int rc;

  /* Evaluate if we can use inplace pointers */ 
  rc = tbl_get_field_indecies(pCur, pTab->nCol, TBL_FDELIM);
  if(rc == SQLITE_OK){
    pCur->iRowid++;
    return SQLITE_OK;
  }

  for(i = 0; i < pTab->nCol; i++){
    pCur->azPtr[i] = 0;
  }

  i = 0;

  do{
    z = tbl_read_one_field(&pCur->rdr,TBL_FDELIM);
    //if( z==0 ){ break; }
    if(pCur->rdr.cTerm==EOF) { break; }

    if( i<pTab->nCol ){
      if( CSV_UNLIKELY(pCur->aLen[i] < pCur->rdr.n+1 )){
        char *zNew = sqlite3_realloc64(pCur->azVal[i], pCur->rdr.n+1);
        if( zNew==0 ){
          csv_errmsg(&pCur->rdr, "out of memory");
          csv_xfer_error(pTab, &pCur->rdr);
          break;
        }
        pCur->azVal[i] = zNew;
        pCur->aLen[i] = pCur->rdr.n+1;
      }
      memcpy(pCur->azVal[i], z, pCur->rdr.n+1);
      i++;
    }
  }while( pCur->rdr.cTerm!='\n' ); // while( pCur->rdr.cTerm==TBL_FDELIM );

  if( /* z==0 || */ (pCur->rdr.cTerm==EOF && i<pTab->nCol) ){
    pCur->iRowid = -1;
  }else{
    pCur->iRowid++;
    while( i<pTab->nCol ){
      sqlite3_free(pCur->azVal[i]);
      pCur->azVal[i] = 0;
      pCur->aLen[i] = 0;
      pCur->azPtr[i] = 0;
      i++;
    }
  }
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the CsvCursor
** is currently pointing.
*/
static int tbltabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  CsvCursor *pCur = (CsvCursor*)cur;
  CsvTable *pTab = (CsvTable*)cur->pVtab;
  if( i>=0 && i<pTab->nCol){
    if(pCur->azPtr[i]!=0){
      if(pTab->cTypes[i] == SQLITE_AFF_INTEGER){        
        sqlite3_result_int(ctx, sqlite3_atoi(pCur->azPtr[i]));
        //sqlite3_result_int(ctx, atoi(pCur->azPtr[i]));
        //sqlite3_result_text(ctx, pCur->azPtr[i], -1 , SQLITE_STATIC);
      } else {      
        sqlite3_result_text(ctx, pCur->azPtr[i], -1 , SQLITE_STATIC);
      }      
    } else if (pCur->azVal[i]!=0){
      if(pTab->cTypes[i]== SQLITE_AFF_INTEGER){
        sqlite3_result_int(ctx, sqlite3_atoi(pCur->azVal[i]));
        //sqlite3_result_int(ctx, atoi(pCur->azVal[i]));
        //sqlite3_result_text(ctx, pCur->azVal[i], -1 /* pCur->aLen[i] */, SQLITE_STATIC);
      } else {      
        sqlite3_result_text(ctx, pCur->azVal[i], -1 /* pCur->aLen[i] */, SQLITE_STATIC);        
      }
    }
  }

  /*
  if( i>=0 && i<pTab->nCol && pCur->azVal[i]!=0 ){
    if(pTab->cTypes[i]){
      sqlite3_result_int(ctx, atoi(pCur->azVal[i]));
    } else {      
      sqlite3_result_text(ctx, pCur->azVal[i], -1 , SQLITE_STATIC);
      //sqlite3_result_text(ctx, pCur->azVal[i], pCur->aLen[i] -1, SQLITE_STATIC);
    }
  }
  */
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int tbltabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  CsvCursor *pCur = (CsvCursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int tbltabEof(sqlite3_vtab_cursor *cur){
  CsvCursor *pCur = (CsvCursor*)cur;
  return pCur->iRowid<0;
}

/*
** Only a full table scan is supported.  So xFilter simply rewinds to
** the beginning.
*/
static int tbltabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  CsvCursor *pCur = (CsvCursor*)pVtabCursor;
  CsvTable *pTab = (CsvTable*)pVtabCursor->pVtab;
  pCur->iRowid = 0;
  if( pCur->rdr.in==0 ){
    assert( pCur->rdr.zIn==pTab->zData );
    assert( pTab->iStart>=0 );
    assert( (size_t)pTab->iStart<=pCur->rdr.nIn );
    pCur->rdr.iIn = pTab->iStart;
  }else{
    /*
    pthread_mutex_lock(&pCur->rdr.lock);
    fseek(pCur->rdr.in, pTab->iStart, SEEK_SET);
    pCur->rdr.iIn = 0;
    pCur->rdr.nIn = 0;
    pthread_mutex_unlock(&pCur->rdr.lock);
    */
  }
  return tbltabNext(pVtabCursor);
}

/*
** Only a forward full table scan is supported.  xBestIndex is mostly
** a no-op.  If CSVTEST_FIDX is set, then the presence of equality
** constraints lowers the estimated cost, which is fiction, but is useful
** for testing certain kinds of virtual table behavior.
*/
static int tbltabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = 1000000;
#ifdef SQLITE_TEST
  if( (((CsvTable*)tab)->tstFlags & CSVTEST_FIDX)!=0 ){
    /* The usual (and sensible) case is to always do a full table scan.
    ** The code in this branch only runs when testflags=1.  This code
    ** generates an artifical and unrealistic plan which is useful
    ** for testing virtual table logic but is not helpful to real applications.
    **
    ** Any ==, LIKE, or GLOB constraint is marked as usable by the virtual
    ** table (even though it is not) and the cost of running the virtual table
    ** is reduced from 1 million to just 10.  The constraints are *not* marked
    ** as omittable, however, so the query planner should still generate a
    ** plan that gives a correct answer, even if they plan is not optimal.
    */
    int i;
    int nConst = 0;
    for(i=0; i<pIdxInfo->nConstraint; i++){
      unsigned char op;
      if( pIdxInfo->aConstraint[i].usable==0 ) continue;
      op = pIdxInfo->aConstraint[i].op;
      if( op==SQLITE_INDEX_CONSTRAINT_EQ 
       || op==SQLITE_INDEX_CONSTRAINT_LIKE
       || op==SQLITE_INDEX_CONSTRAINT_GLOB
      ){
        pIdxInfo->estimatedCost = 10;
        pIdxInfo->aConstraintUsage[nConst].argvIndex = nConst+1;
        nConst++;
      }
    }
  }
#endif
  return SQLITE_OK;
}


static sqlite3_module TblModule = {
  0,                       /* iVersion */
  tbltabCreate,            /* xCreate */
  tbltabConnect,           /* xConnect */
  tbltabBestIndex,         /* xBestIndex */
  tbltabDisconnect,        /* xDisconnect */
  tbltabDisconnect,        /* xDestroy */
  tbltabOpen,              /* xOpen - open a cursor */
  tbltabClose,             /* xClose - close a cursor */
  tbltabFilter,            /* xFilter - configure scan constraints */
  tbltabNext,              /* xNext - advance a cursor */
  tbltabEof,               /* xEof - check for end of scan */
  tbltabColumn,            /* xColumn - read data */
  tbltabRowid,             /* xRowid - read data */
  0,                       /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};

#ifdef SQLITE_TEST
/*
** For virtual table testing, make a version of the CSV virtual table
** available that has an xUpdate function.  But the xUpdate always returns
** SQLITE_READONLY since the CSV file is not really writable.
*/
static int tbltabUpdate(sqlite3_vtab *p,int n,sqlite3_value**v,sqlite3_int64*x){
  return SQLITE_READONLY;
}
static sqlite3_module CsvModuleFauxWrite = {
  0,                       /* iVersion */
  tbltabCreate,            /* xCreate */
  tbltabConnect,           /* xConnect */
  tbltabBestIndex,         /* xBestIndex */
  tbltabDisconnect,        /* xDisconnect */
  tbltabDisconnect,        /* xDestroy */
  tbltabOpen,              /* xOpen - open a cursor */
  tbltabClose,             /* xClose - close a cursor */
  tbltabFilter,            /* xFilter - configure scan constraints */
  tbltabNext,              /* xNext - advance a cursor */
  tbltabEof,               /* xEof - check for end of scan */
  tbltabColumn,            /* xColumn - read data */
  tbltabRowid,             /* xRowid - read data */
  tbltabUpdate,            /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};
#endif /* SQLITE_TEST */

#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) */


#ifdef _WIN32
__declspec(dllexport)
#endif
/* 
** This routine is called when the extension is loaded.  The new
** CSV virtual table module is registered with the calling database
** connection.
*/
int sqlite3_tbl_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
#ifndef SQLITE_OMIT_VIRTUALTABLE	
  int rc;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "tbl", &TblModule, 0);
#ifdef SQLITE_TEST
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "csv_wr", &CsvModuleFauxWrite, 0);
  }
#endif
  return rc;
#else
  return SQLITE_OK;
#endif
}
