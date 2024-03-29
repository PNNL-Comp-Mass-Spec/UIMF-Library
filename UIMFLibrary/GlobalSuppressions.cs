﻿
// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("General", "RCS1079:Throwing of new NotImplementedException.", Justification = "Ignore here", Scope = "member", Target = "~M:UIMFLibrary.DataReader.GetUpperLowerBinsFromMz(System.Int32,System.Double,System.Double)~System.Int32[]")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.BinCentricTableCreation.CreateIndexes(System.String,System.Int32)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.BinCentricTableCreation.CreateTemporaryDatabase(UIMFLibrary.DataReader,System.String)~System.String")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.BinCentricTableCreation.InsertBinCentricData(System.Data.SQLite.SQLiteConnection,System.Data.SQLite.SQLiteConnection,UIMFLibrary.DataReader)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.BinCentricTableCreation.SortDataForBin(System.Data.SQLite.SQLiteConnection,System.Data.SQLite.SQLiteCommand,System.Int32,System.Int32)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.CLZF2.LZF_Compress(System.Byte[],System.Byte[]@,System.Int32)~System.Int32")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.DataReader.AccumulateFrameData(System.Int32,System.Int32,System.Boolean,System.Int32,System.Int32,System.Int32,System.Int32,System.Double,System.Double)~System.Double[,]")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.MzCalibrator.MZtoTOF(System.Double)~System.Int32")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.UIMFData.ConvertBinToMZ(System.Double,System.Double,System.Double,System.Double,System.Int32)~System.Double")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.UIMFData.ConvertBinToMz(System.Int32,System.Double,UIMFLibrary.FrameParams)~System.Double")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:UIMFLibrary.UIMFData.GetBinClosestToMZ(System.Double,System.Double,System.Double,System.Double,System.Double)~System.Double")]
[assembly: SuppressMessage("Roslynator", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:UIMFLibrary.UIMFData.GetGlobalParametersFromLegacyTable(System.Data.SQLite.SQLiteConnection)~UIMFLibrary.GlobalParams")]
[assembly: SuppressMessage("Roslynator", "RCS1163:Unused parameter.", Justification = "Leave parameter for compatibility", Scope = "member", Target = "~M:UIMFLibrary.DataReader.AccumulateFrameData(System.Int32,System.Int32,System.Boolean,System.Int32,System.Int32,System.Int32,System.Int32,System.Double,System.Double)~System.Double[,]")]
[assembly: SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "Leave parameter for compatibility", Scope = "member", Target = "~M:UIMFLibrary.DataReader.AccumulateFrameData(System.Int32,System.Int32,System.Boolean,System.Int32,System.Int32,System.Int32,System.Int32,System.Double,System.Double)~System.Double[,]")]
[assembly: SuppressMessage("Style", "IDE0270:Use coalesce expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:UIMFLibrary.DataReader.GetFrameTypeForFrame(System.Int32)~UIMFLibrary.UIMFData.FrameType")]
[assembly: SuppressMessage("Style", "IDE0270:Use coalesce expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:UIMFLibrary.DataReader.ReadFrameParamValue(System.Data.IDataRecord,System.Int32,System.Int32,System.Collections.Generic.IReadOnlyDictionary{UIMFLibrary.FrameParamKeyType,UIMFLibrary.FrameParamDef},UIMFLibrary.FrameParams)")]
[assembly: SuppressMessage("Style", "IDE0270:Use coalesce expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:UIMFLibrary.UIMFData.CacheGlobalParameters")]
