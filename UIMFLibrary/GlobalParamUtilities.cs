using System;
using System.Collections.Generic;
using System.Linq;

namespace UIMFLibrary
{
    /// <summary>
    /// Utility functions for working with Global parameters
    /// </summary>
    public static class GlobalParamUtilities
    {
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
        /// Create a Global parameter dictionary using a GlobalParameters class instance
        /// </summary>
        /// <param name="globalParameters"></param>
        /// <returns>Global parameter dictionary</returns>
#pragma warning disable 612, 618
        public static Dictionary<GlobalParamKeyType, string> ConvertGlobalParameters(GlobalParameters globalParameters)
#pragma warning restore 612, 618
        {
            int prescanContinuous = 0;

            if (globalParameters.Prescan_Continuous)
                prescanContinuous = 1;

            var GlobalParams = new Dictionary<GlobalParamKeyType, string>
            { 
                {GlobalParamKeyType.InstrumentName, globalParameters.InstrumentName},
                {GlobalParamKeyType.DateStarted, globalParameters.DateStarted},
                {GlobalParamKeyType.NumFrames, UIMFDataUtilities.IntToString(globalParameters.NumFrames)},
                {GlobalParamKeyType.TimeOffset, UIMFDataUtilities.IntToString(globalParameters.TimeOffset)},
                {GlobalParamKeyType.BinWidth, UIMFDataUtilities.DoubleToString(globalParameters.BinWidth)},
                {GlobalParamKeyType.Bins, UIMFDataUtilities.IntToString(globalParameters.Bins)},
                {GlobalParamKeyType.TOFCorrectionTime, UIMFDataUtilities.DoubleToString(globalParameters.TOFCorrectionTime)},
                // Obsolete: {GlobalParamKeyType.FrameDataBlobVersion, UIMFDataUtilities.DoubleToString(globalParameters.FrameDataBlobVersion)},
                // Obsolete: {GlobalParamKeyType.ScanDataBlobVersion, UIMFDataUtilities.DoubleToString(globalParameters.ScanDataBlobVersion)},
                {GlobalParamKeyType.TOFIntensityType, globalParameters.TOFIntensityType},
                {GlobalParamKeyType.DatasetType, globalParameters.DatasetType},
                {GlobalParamKeyType.PrescanTOFPulses, UIMFDataUtilities.IntToString(globalParameters.Prescan_TOFPulses)},
                {GlobalParamKeyType.PrescanAccumulations, UIMFDataUtilities.IntToString(globalParameters.Prescan_Accumulations)},
                {GlobalParamKeyType.PrescanTICThreshold, UIMFDataUtilities.IntToString(globalParameters.Prescan_TICThreshold)},
                {GlobalParamKeyType.PrescanContinuous, UIMFDataUtilities.IntToString(prescanContinuous)},
                {GlobalParamKeyType.PrescanProfile, globalParameters.Prescan_Profile}
	        };

            return GlobalParams;
        }

        /// <summary>
        /// Convert a Global parameter dictionary to an instance of the <see cref="GlobalParams"/> class
        /// </summary>
        /// <param name="GlobalParamsByType"></param>
        /// <returns></returns>
        public static GlobalParams ConvertStringParamsToGlobalParams(Dictionary<GlobalParamKeyType, string> GlobalParamsByType)
        {
            var GlobalParams = new GlobalParams();

            foreach (var paramItem in GlobalParamsByType)
            {
                GlobalParams.AddUpdateValue(paramItem.Key, paramItem.Value);
            }

            return GlobalParams;
        }

        /// <summary>
        /// Lookup the official .NET data type given the string name of a data type
        /// </summary>
        /// <param name="alias"></param>
        /// <returns>Official .NET data type</returns>
        /// <remarks>Returns System.Object if not match</remarks>
        public static string GetDataTypeFromAlias(string alias)
        {
            string systemDataType;
            if (mDataTypeAliasMap.TryGetValue(alias, out systemDataType))
                return systemDataType;

            Console.WriteLine("Warning: data type alias not recognized: " + alias);
            return "System.Object";
        }

#pragma warning disable 612, 618
        /// <summary>
        /// Obtain a GlobalParameters instance from a GlobalParams instance
        /// </summary>
        /// <param name="globalParams"><see cref="GlobalParams"/> instance</param>
        /// <returns>A new <see cref="GlobalParameters"/> instance</returns>
        public static GlobalParameters GetLegacyGlobalParameters(GlobalParams globalParams)
        {
            if (globalParams == null)
                return new GlobalParameters();

            // PrescanContinuous is a boolean value stored as a 0 or 1
            int result = globalParams.GetValueInt32(GlobalParamKeyType.PrescanContinuous, 0);
            bool prescanContinuous = (result != 0);

            // Populate legacyGlobalParams using dictionary GlobalParams
            var legacyGlobalParams = new GlobalParameters
            {
                InstrumentName = globalParams.GetValue(GlobalParamKeyType.InstrumentName),
                DateStarted = globalParams.GetValue(GlobalParamKeyType.DateStarted),
                NumFrames = globalParams.GetValueInt32(GlobalParamKeyType.NumFrames, 0),
                TimeOffset = globalParams.GetValueInt32(GlobalParamKeyType.TimeOffset, 0),
                BinWidth = globalParams.GetValueDouble(GlobalParamKeyType.BinWidth, 0),
                Bins = globalParams.GetValueInt32(GlobalParamKeyType.Bins, 0),
                TOFCorrectionTime = (float)globalParams.GetValueDouble(GlobalParamKeyType.TOFCorrectionTime, 0),
                FrameDataBlobVersion = 0.1f,      // Legacy parameter that was always 0.1
                ScanDataBlobVersion = 0.1f,       // Legacy parameter that was always 0.1
                TOFIntensityType = globalParams.GetValue(GlobalParamKeyType.TOFIntensityType),
                DatasetType = globalParams.GetValue(GlobalParamKeyType.DatasetType),
                Prescan_TOFPulses = globalParams.GetValueInt32(GlobalParamKeyType.PrescanTOFPulses, 0),
                Prescan_Accumulations = globalParams.GetValueInt32(GlobalParamKeyType.PrescanAccumulations, 0),
                Prescan_TICThreshold = globalParams.GetValueInt32(GlobalParamKeyType.PrescanTICThreshold, 0),
                Prescan_Continuous = prescanContinuous,
                Prescan_Profile = globalParams.GetValue(GlobalParamKeyType.PrescanProfile)
            };

            return legacyGlobalParams;
        }
#pragma warning restore 612, 618

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
        public static GlobalParamKeyType GetParamTypeByName(string paramName)
        {
            int iteration = 0;

            // On the first iteration of the while loop, we require a case-sensitive match
            // If no match is found, then on the second iteration we use a case-insensitive match
            while (iteration < 2)
            {

                try
                {
                    // Note that this conversion works for both the names in the GlobalParamKeyType enum and for the integer values
                    // See MSDN's "Enum.Parse Method" page at http://msdn.microsoft.com/en-us/library/essfb559.aspx
                    bool ignoreCase = iteration > 0;
                    var paramType = (GlobalParamKeyType)Enum.Parse(typeof(GlobalParamKeyType), paramName, ignoreCase);
                    if (Enum.IsDefined(typeof(GlobalParamKeyType), paramType) | paramType.ToString().Contains(","))
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

                iteration++;
            }

            return GlobalParamKeyType.Unknown;

        }

    }

}
