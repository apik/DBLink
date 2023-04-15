#include "WolframLibrary.h"
#include "WolframIOLibraryFunctions.h"
#include <kchashdb.h>
#include <unordered_map>


using std::cout;
using std::cerr;
using std::endl;
using std::string;

class DB
{
  kyotocabinet::HashDB db;
public:

  DB() = default;
  
  bool open(const std::string & dbpath, bool db_replace)
  {
    if (db_replace)
      return db.open(dbpath, kyotocabinet::HashDB::OWRITER | kyotocabinet::HashDB::OTRUNCATE);
    else
      return db.open(dbpath, kyotocabinet::HashDB::OWRITER | kyotocabinet::HashDB::OCREATE);
  }

  bool read(const std::string & dbpath)
  {
    return db.open(dbpath, kyotocabinet::HashDB::OREADER); 
  }

  // Overwrite if key exists
  bool set(const std::string & key, const std::string & value)
  {
    return db.set(key, value);
  }

  // Do nothing if key exists
  bool add(const std::string & key, const std::string & value)
  {
    return db.add(key, value);
  }


  bool get(const std::string & key, std::string & value)
  {
    return db.get(key, &value);
  }

  bool has_key(const std::string & key, std::string & value)
  {
    return db.get(key, &value);
  }

  mint size()
  {
    return db.count();
  }

  void error()
  {
    cerr << "Error: " << db.error().name() << endl;
  }

  ~DB()
  {
    if (!db.close())
      {
        cerr << "DB close error: " << db.error().name() << endl;
      }
  };
};


typedef std::unordered_map<mint, DB*> DBMap;
static DBMap map;

EXTERN_C DLLEXPORT void manage_instance(WolframLibraryData libData, mbool mode, mint id)
{
  if (mode == 0)
    {
      DB *T = new(DB);
      map[id] = T;
    }
  else
    {
      DB *T = map[id];
      if (T != nullptr)
        {           
          delete T;       
          map.erase(id);       
        }
    }
}


EXTERN_C DLLEXPORT int DBOpen(WolframLibraryData libData, mint Argc, MArgument *Args, MArgument res)
{
  if (Argc != 3) return LIBRARY_FUNCTION_ERROR;
  mint id = MArgument_getInteger(Args[0]);
  
  DB *T = map[id];
  if (T == nullptr) return LIBRARY_FUNCTION_ERROR;
  
  std::string dbpath(MArgument_getUTF8String(Args[1]));

  mbool replace_db = MArgument_getBoolean(Args[2]);

  if(!T->open(dbpath, replace_db > 0)) return LIBRARY_FUNCTION_ERROR;
  
  return LIBRARY_NO_ERROR;
}

EXTERN_C DLLEXPORT int DBRead(WolframLibraryData libData, mint Argc, MArgument *Args, MArgument res)
{
  if (Argc != 2) return LIBRARY_FUNCTION_ERROR;
  mint id = MArgument_getInteger(Args[0]);
  
  DB *T = map[id];
  if (T == nullptr) return LIBRARY_FUNCTION_ERROR;
  
  std::string dbpath(MArgument_getUTF8String(Args[1]));
  
  if(!T->read(dbpath)) return LIBRARY_FUNCTION_ERROR;
  
  return LIBRARY_NO_ERROR;
}

EXTERN_C DLLEXPORT int DBClose(WolframLibraryData libData, mint Argc, MArgument *Args, MArgument res)
{
  if (Argc != 1) return LIBRARY_FUNCTION_ERROR;
  mint id = MArgument_getInteger(Args[0]);
  
  DB *T = map[id];
  if (T == nullptr) return LIBRARY_FUNCTION_ERROR;

  delete T;       
  map.erase(id);       

  return LIBRARY_NO_ERROR;
}

EXTERN_C DLLEXPORT int DBGet(WolframLibraryData libData, mint Argc, MArgument *Args, MArgument res)
{
  if (Argc != 2) return LIBRARY_FUNCTION_ERROR;
  mint id = MArgument_getInteger(Args[0]);
  
  DB *T = map[id];
  if (T == nullptr) return LIBRARY_FUNCTION_ERROR;
  
  std::string key(MArgument_getUTF8String(Args[1]));
  std::string value;

  DataStore ds_out = libData->ioLibraryFunctions->createDataStore();
  if (ds_out == nullptr) return LIBRARY_FUNCTION_ERROR;


  if(T->get(key,value))
    {
      // count=1
      libData->ioLibraryFunctions->DataStore_addInteger(ds_out, 1);
      libData->ioLibraryFunctions->DataStore_addString(ds_out, const_cast<char*>(value.c_str()));
    }
  else
    {
      // count=0
      libData->ioLibraryFunctions->DataStore_addInteger(ds_out, 0);
      libData->ioLibraryFunctions->DataStore_addString(ds_out, const_cast<char*>(""));
    }

  MArgument_setDataStore(res, ds_out);
  
  return LIBRARY_NO_ERROR;
}

EXTERN_C DLLEXPORT int DBSet(WolframLibraryData libData, mint Argc, MArgument *Args, MArgument res)
{
  if (Argc != 4) return LIBRARY_FUNCTION_ERROR;
  mint id = MArgument_getInteger(Args[0]);
  
  DB *T = map[id];
  if (T == nullptr) return LIBRARY_FUNCTION_ERROR;
   
  std::string   key(MArgument_getUTF8String(Args[1]));
  std::string value(MArgument_getUTF8String(Args[2]));

  mbool do_overwrite = MArgument_getBoolean(Args[3]);
  if (do_overwrite)
    {
      if(!T->set(key,value))
        return LIBRARY_FUNCTION_ERROR;
      else
        MArgument_setBoolean(res,True);
    }
  else
    {
      if(T->add(key,value))
        MArgument_setBoolean(res,True);
      else
        MArgument_setBoolean(res,False);
    }

  return LIBRARY_NO_ERROR;
}

EXTERN_C DLLEXPORT int DBSize(WolframLibraryData libData, mint Argc, MArgument *Args, MArgument res)
{
  if (Argc != 1) return LIBRARY_FUNCTION_ERROR;
  mint id = MArgument_getInteger(Args[0]);
  
  DB *T = map[id];
  if (T == nullptr) return LIBRARY_FUNCTION_ERROR;

  MArgument_setInteger(res, T->size());
  return LIBRARY_NO_ERROR;
}

EXTERN_C DLLEXPORT mint WolframLibrary_getVersion()
{
  return WolframLibraryVersion;
}

EXTERN_C DLLEXPORT int WolframLibrary_initialize(WolframLibraryData libData)
{
  return libData->registerLibraryExpressionManager("DB", manage_instance);
}

EXTERN_C DLLEXPORT void WolframLibrary_uninitialize(WolframLibraryData libData)
{
  int err = libData->unregisterLibraryExpressionManager("DB");
}
