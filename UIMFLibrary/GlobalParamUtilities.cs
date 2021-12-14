using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    /// <summary>
    /// Utility functions for working with Global parameters
    /// </summary>
    public static class GlobalParamUtilities
    {
        // Ignore Spelling: bool, uint, ulong, ushort, sbyte

        private static readonly Dictionary<GlobalParamKeyType, Type> mGlobalParamKeyTypes = new Dictionary<GlobalParamKeyType, Type>();

        /// <summary>
        /// Map between .net data type aliases and official data type names
        /// </summary>
        public static readonly Dictionary<string, string> mDataTypeAliasMap =
            new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase)
            {
                {"bool", "System.Boolean"},
                {"byte", "System.Byte"},
                {"sbyte", "System.SByte"},
                {"char", "System.Char"},
                {"decimal", "System.Decimal"},
                {"double", "System.Double"},
                {"float", "System.Single"},
                {"int", "System.Int32"},
                {"uint", "System.UInt32"},
                {"long", "System.Int64"},
                {"ulong", "System.UInt64"},
                {"object", "System.Object"},
                {"short", "System.Int16"},
                {"ushort", "System.UInt16"},
                {"string", "System.String"},
                {"Boolean", "System.Boolean"},
                {"Single", "System.Single"},
                {"Int32", "System.Int32"},
                {"UInt32", "System.UInt32"},
                {"Int64", "System.Int64"},
                {"UInt64", "System.UInt64"},
                {"Int16", "System.Int16"},
                {"UInt16", "System.UInt16"}
            };

        /// <summary>
        /// Lookup the official .NET data type given the string name of a data type
        /// </summary>
        /// <remarks>Returns System.Object if not match</remarks>
        /// <param name="alias"></param>
        /// <returns>Official .NET data type</returns>
        public static string GetDataTypeFromAlias(string alias)
        {
            if (mDataTypeAliasMap.TryGetValue(alias, out var systemDataType))
                return systemDataType;

            Console.WriteLine("Warning: data type alias not recognized: " + alias);
            return "System.Object";
        }

        /// <summary>
        /// Get the default value for the data type associated with the given frame param key
        /// </summary>
        /// <param name="paramType"></param>
        public static dynamic GetDefaultValueByType(GlobalParamKeyType paramType)
        {
            var dataType = GetGlobalParamKeyDataType(paramType);
            return FrameParamUtilities.GetDefaultValueByType(dataType);
        }

        /// <summary>
        /// Get the system data type associated with a given global parameter key
        /// </summary>
        /// <param name="paramType"></param>
        public static Type GetGlobalParamKeyDataType(GlobalParamKeyType paramType)
        {
            if (mGlobalParamKeyTypes.Count == 0)
            {
                var keyTypes = new Dictionary<GlobalParamKeyType, Type>
                {
                    {GlobalParamKeyType.InstrumentName, typeof(string)},
                    {GlobalParamKeyType.DateStarted, typeof(string)},            // Stored as a string to assure the format does not change
                    {GlobalParamKeyType.NumFrames, typeof(int)},
                    {GlobalParamKeyType.TimeOffset, typeof(int)},
                    {GlobalParamKeyType.BinWidth, typeof(double)},               // TOF-bin size (in nanoseconds) or ppm bin size (in parts-per-million)
                    {GlobalParamKeyType.Bins, typeof(int)},
                    {GlobalParamKeyType.TOFCorrectionTime, typeof(float)},
                    {GlobalParamKeyType.TOFIntensityType, typeof(string)},
                    {GlobalParamKeyType.DatasetType, typeof(string)},
                    {GlobalParamKeyType.PrescanTOFPulses, typeof(int)},
                    {GlobalParamKeyType.PrescanAccumulations, typeof(int)},
                    {GlobalParamKeyType.PrescanTICThreshold, typeof(int)},
                    {GlobalParamKeyType.PrescanContinuous, typeof(int)},
                    {GlobalParamKeyType.PrescanProfile, typeof(string)},
                    {GlobalParamKeyType.InstrumentClass, typeof(int)},          // 0 for TOF; 1 for ppm bin-based
                    {GlobalParamKeyType.PpmBinBasedStartMz, typeof(double)},    // Only used when InstrumentClass is 1 (ppm bin-based)
                    {GlobalParamKeyType.PpmBinBasedEndMz, typeof(double)},      // Only used when InstrumentClass is 1 (ppm bin-based)
                    {GlobalParamKeyType.DriftTubeLength, typeof(double)},       // Only used for IMS
                    {GlobalParamKeyType.DriftGas, typeof(string)},
                    {GlobalParamKeyType.ADCName, typeof(string)},
                    {GlobalParamKeyType.OneBasedDriftScans, typeof(bool)},
                    {GlobalParamKeyType.AcquisitionMethod, typeof(string)},
                };

                foreach (var item in keyTypes)
                    mGlobalParamKeyTypes.Add(item.Key, item.Value);
            }

            if (mGlobalParamKeyTypes.TryGetValue(paramType, out var dataType))
                return dataType;

            throw new ArgumentOutOfRangeException(nameof(paramType), "Unrecognized frame param enum for paramType: " + (int)paramType);
        }

        /// <summary>
        /// Resolve GlobalParam Key Type using the parameter id integer value
        /// </summary>
        /// <param name="paramID"></param>
        /// <returns>Specific GlobalParamKeyType enum, or GlobalParamKeyType.Unknown</returns>
        public static GlobalParamKeyType GetParamTypeByID(int paramID)
        {
            if (Enum.IsDefined(typeof(GlobalParamKeyType), paramID))
                return (GlobalParamKeyType)paramID;

            return GlobalParamKeyType.Unknown;
        }

        /// <summary>
        /// Resolve GlobalParam Key Type using the parameter name
        /// </summary>
        /// <param name="paramName"></param>
        /// <returns>Specific GlobalParamKeyType enum, or GlobalParamKeyType.Unknown</returns>
        // ReSharper disable once UnusedMember.Global
        public static GlobalParamKeyType GetParamTypeByName(string paramName)
        {
            // On the first iteration of the while loop, we require a case-sensitive match
            // If no match is found, then on the second iteration we use a case-insensitive match
            for (var iteration = 0; iteration < 2; iteration++)
            {
                try
                {
                    // Note that this conversion works for both the names in the GlobalParamKeyType enum and for the integer values
                    // See MSDN's "Enum.Parse Method" page at http://msdn.microsoft.com/en-us/library/essfb559.aspx
                    var ignoreCase = iteration > 0;
                    var paramType = (GlobalParamKeyType)Enum.Parse(typeof(GlobalParamKeyType), paramName, ignoreCase);
                    if (Enum.IsDefined(typeof(GlobalParamKeyType), paramType) || paramType.ToString().Contains(","))
                    {
                        // Match found
                        return paramType;
                    }

                    // No match

                }
                catch (ArgumentException)
                {
                    // No match
                }
            }

            return GlobalParamKeyType.Unknown;
        }
    }
}
