using System;

namespace UIMFLibrary
{
    #region Global Parameter Enum

    /// <summary>
    /// Known global parameters
    /// </summary>
    public enum GlobalParamKeyType
    {
        Unknown = 0,
        InstrumentName = 1,
        DateStarted = 2,
        NumFrames = 3,
        TimeOffset = 4,
        BinWidth = 5,
        Bins = 6,
        TOFCorrectionTime = 7,
        TOFIntensityType = 8,
        DatasetType = 9,
        PrescanTOFPulses = 10,
        PrescanAccumulations = 11,
        PrescanTICThreshold = 12,
        PrescanContinuous = 13,
        PrescanProfile = 14
    }

    #endregion

    public class GlobalParam
    {

        /// <summary>
        /// Parameter Type
        /// </summary>
        public GlobalParamKeyType ParamType { get; private set; }

        /// <summary>
        /// Parameter Name
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// .NET data type
        /// </summary>
        public Type DataType { get; private set; }

        /// <summary>
        /// Parameter Description
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Parameter value
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramType">Frame parameter definition</param>
        /// <param name="value">Parameter value</param>
        public GlobalParam(GlobalParamKeyType paramType, string value)
        {
            InitializeByType(paramType);
            Value = value;
        }

        /// <summary>
        /// Customized ToString()
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (Value == null)
                return ParamType + " (" + DataType + ")";

            return Value;
        }

        /// <summary>
        /// Initialize this global parameter using the param type enum value
        /// </summary>
        /// <param name="paramType">Param key type enum</param>
        private void InitializeByType(GlobalParamKeyType paramType)
        {
            ParamType = paramType;

            switch (paramType)
            {

                case GlobalParamKeyType.InstrumentName:
                    InitializeByType("InstrumentName", "string", "Instrument name");
                    break;

                case GlobalParamKeyType.DateStarted:
                    // Format has traditionally been M/d/yyyy hh:mm:ss tt
                    // For example, 6/4/2014 12:56:44 PM
                    InitializeByType("DateStarted", "string", "Time that the data acquisition started");
                    break;

                case GlobalParamKeyType.NumFrames:
                    InitializeByType("NumFrames", "int", "Number of frames in the dataset");
                    break;

                case GlobalParamKeyType.TimeOffset:
                    InitializeByType("TimeOffset", "int", "Time offset from 0 (in nanoseconds). All bin numbers must be offset by this amount");
                    break;

                case GlobalParamKeyType.BinWidth:
                    InitializeByType("BinWidth", "double", "Width of TOF bins (in ns)");
                    break;

                case GlobalParamKeyType.Bins:
                    InitializeByType("Bins", "int", "Total number of TOF bins in frame");
                    break;

                case GlobalParamKeyType.TOFCorrectionTime:
                    InitializeByType("TOFCorrectionTime", "float", "TOF correction time");
                    break;

                case GlobalParamKeyType.TOFIntensityType:
                    InitializeByType("TOFIntensityType", "string", "Data type of intensity in each TOF record (ADC is int, TDC is short, FOLDED is float)");
                    break;

                case GlobalParamKeyType.DatasetType:
                    InitializeByType("DatasetType", "string", "Type of dataset (HMS, HMSn, or HMS-HMSn)");
                    break;

                case GlobalParamKeyType.PrescanTOFPulses:
                    InitializeByType("PrescanTOFPulses", "string", "Prescan TOF pulses");
                    break;

                case GlobalParamKeyType.PrescanAccumulations:
                    InitializeByType("PrescanAccumulations", "string", "Number of prescan accumulations");
                    break;

                case GlobalParamKeyType.PrescanTICThreshold:
                    InitializeByType("PrescanTICThreshold", "string", "Prescan TIC threshold");
                    break;

                case GlobalParamKeyType.PrescanContinuous:
                    InitializeByType("PrescanContinuous", "int", "Prescan Continuous flag (0 is false, 1 is true)");
                    break;

                case GlobalParamKeyType.PrescanProfile:
                    InitializeByType("PrescanProfile", "string", "Profile used when PrescanContinuous is 1");
                    break;

                default:
                    throw new ArgumentOutOfRangeException("paramType", "Unrecognized global param enum for paramType: " + (int)paramType);
            }

        }

        private void InitializeByType(string name, string dataType, string description)
        {
            Name = name;
            DataType = GetDataTypeFromAlias(dataType);
            Description = description;
        }

        private Type GetDataTypeFromAlias(string dataType)
        {
            dataType = GlobalParamUtilities.GetDataTypeFromAlias(dataType);

            return Type.GetType(dataType) ?? typeof(object);
        }

    }
}
