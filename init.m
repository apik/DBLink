Unprotect["DBLink`*"];
ClearAll["DBLink`*"];
If[TrueQ[$VersionNumber < 12.0],
   Print["Minimum Mathematica version required for \"DBLink\" is 12.0, version "
         <>ToString[$VersionNumber,InputForm]<>" found, exiting!"];
   Abort[]];


BeginPackage["DBLink`"];

db$Open::usage="db$Open[\"dbname\", \"dbpath\"] - open DB stored in the file \"dbpath\" for modification.
                        Options:
                               \"Replace\" - rewrite existing DB file
                               \"Append\"  - modify existing DB file";

db$Read::usage="db$Read[\"dbname\", \"dbpath\"] - open DB stored in the file \"dbpath\" in readonly mode.";
db$Close::usage="db$Close[\"dbname\"] - Close DB with name \"dbname\".";

db$Set::usage="db$Set[\"dbname\", \"key\" -> \"value\"] - set \"key\"=\"value\".
                        Options:
                               \"Force\" - owervrite existing key value";
db$Get::usage="db$Get[\"dbname\", \"key\"] - Retrive value for specific key \"key\".
                        Options:
                               \"NotFoundValue\" - expression to return, when key not found.";

db$Size::usage="db$Size[\"dbname\"] - get number of keys in DB.";

Begin["`Private`"];
AppendTo[$Path, DirectoryName[$InputFileName]];
(* ------------------------------------------------------------------ *)

DBOpen  = LibraryFunctionLoad["lib/DBLink.so", 
                              "DBOpen",
                              {Integer, "UTF8String", "Boolean"},
                              "Void"];

DBRead  = LibraryFunctionLoad["lib/DBLink.so", 
                              "DBRead",
                              {Integer, "UTF8String"},
                              "Void"];

DBClose = LibraryFunctionLoad["lib/DBLink.so", 
                              "DBClose",
                              {Integer},
                              "Void"];

DBSet   = LibraryFunctionLoad["lib/DBLink.so", 
                              "DBSet",
                              {Integer, "UTF8String", "UTF8String", "Boolean"},
                              "Boolean"];

DBGet   = LibraryFunctionLoad["lib/DBLink.so",
                              "DBGet",
                              {Integer, "UTF8String"},
                              "DataStore"];

DBSize  = LibraryFunctionLoad["lib/DBLink.so",
                              "DBSize",
                              {Integer},
                              Integer];

(* ------------------------------------------------------------------ *)

General::nodb="DB with name \"`1`\" not found";
db$Info=Association[];

db$Open::dbname="DB with name \"`1`\" already exists";
db$Open::dbpath="DB \"`1`\" already exists, use \"Replace\" or \"Append\" options";
Options[db$Open] = {"Replace" -> False, "Append" -> True};
db$Open[dbname_, dbpath_String, OptionsPattern[]] :=
        Block[{$HistoryLength = 0},
              If[KeyExistsQ[db$Info,dbname],
                 Message[db$Open::dbname, dbname];
                 Abort[],

                 If[FileExistsQ[dbpath] && Not[TrueQ[OptionValue["Replace"]]] && Not[TrueQ[OptionValue["Append"]]],
                    Message[db$Open::dbpath,dbpath];
                    Abort[]];
                 (* Open DB *)
                 db$Info[dbname] = CreateManagedLibraryExpression["DB", DBL];
                 DBOpen[ManagedLibraryExpressionID@db$Info[dbname], dbpath, TrueQ[OptionValue["Replace"]]];
                 dbname
              ]              
        ];

db$Read::dbname="DB with name \"`1`\" already exists";
db$Read[dbname_, dbpath_String] :=
        Block[{$HistoryLength = 0},
              If[KeyExistsQ[db$Info,dbname],
                 Message[db$Read::dbname, dbname];
                 Abort[],
                 (* Open DB *)
                 db$Info[dbname] = CreateManagedLibraryExpression["DB", DBL];
                 DBRead[ManagedLibraryExpressionID@db$Info[dbname], dbpath];
                 dbname
              ]              
        ];


db$Close[dbname_] :=
        Block[{},
              If[KeyExistsQ[db$Info,dbname],
                 (* Close DB *)
                 DBClose[ManagedLibraryExpressionID@db$Info[dbname]];
               ,
                 Message[General::nodb, dbname];
              ]
        ];

Options[db$Set] = {"Force" -> False};
db$Set[dbname_, key_String -> value_String, OptionsPattern[]] :=
        Block[{},
              If[KeyExistsQ[db$Info,dbname],
                 DBSet[ManagedLibraryExpressionID@db$Info[dbname], key, value, TrueQ[OptionValue["Force"]]]
               ,
                 Message[General::nodb, dbname];
              ]
        ];

Options[db$Get] = {"NotFoundValue" -> None};
db$Get[dbname_, key_String, OptionsPattern[]] :=
        Block[{nkeys,val},
              If[KeyExistsQ[db$Info,dbname],
                 {nkeys,val} = List@@DBGet[ManagedLibraryExpressionID@db$Info[dbname], key];
                 If[nkeys > 0,
                    val,
                    OptionValue["NotFoundValue"]
                 ]
               ,
                 Message[General::nodb, dbname];
                 None
              ]
        ];

db$Size[dbname_] :=
        Block[{},
              If[KeyExistsQ[db$Info,dbname],
                 DBSize[ManagedLibraryExpressionID@db$Info[dbname]]
               ,
                 Message[General::nodb, dbname];
                 None
              ]              
        ]

End[]
(* Private *)
EndPackage[]
