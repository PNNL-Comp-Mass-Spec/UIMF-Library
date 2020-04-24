using System;

namespace UIMFLibrary
{
    #region Global Parameter Enum

    /// <summary>
    /// Known global parameters
    /// </summary>
    public enum GlobalParamKeyType
    {
        /// <summary>
        /// Unknown Global Parameter key
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// Key: Instrument Name
        /// </summary>
        InstrumentName = 1,

        /// <summary>
        /// Key: Date Data collection started
        /// </summary>
        DateStarted = 2,

        /// <summary>
        /// Key: Number of frames
        /// </summary>
        NumFrames = 3,

        /// <summary>
        /// Key: Time offset
        /// </summary>
        TimeOffset = 4,

        /// <summary>
        /// Key: Bin width
        /// </summary>
        BinWidth = 5,               // TOF-bin size (in nanoseconds) or ppm bin size (in parts-per-million)

        /// <summary>
        /// Key: Bins
        /// </summary>
        Bins = 6,

        /// <summary>
        /// Key: TOF Correction Time
        /// </summary>
        // ReSharper disable once InconsistentNaming
        TOFCorrectionTime = 7,

        /// <summary>
        /// Key: TOF Intensity type
        /// </summary>
        TOFIntensityType = 8,

        /// <summary>
        /// Key: Dataset type
        /// </summary>
        DatasetType = 9,

        /// <summary>
        /// Key: Prescan TOF Pulses
        /// </summary>
        /// <remarks>
        /// Tracks the maximum scan number in any frame
        /// </remarks>
        PrescanTOFPulses = 10,

        /// <summary>
        /// Key: Prescan Accumulations
        /// </summary>
        PrescanAccumulations = 11,

        /// <summary>
        /// Key: Prescan TIC Threshold
        /// </summary>
        PrescanTICThreshold = 12,

        /// <summary>
        /// Key: Prescan Continuous
        /// </summary>
        PrescanContinuous = 13,

        /// <summary>
        /// Key: Prescan Profile
        /// </summary>
        PrescanProfile = 14,

        /// <summary>
        /// Key: Instrument Class
        /// </summary>
        InstrumentClass = 15,       // 0 for TOF; 1 for ppm bin-based

        /// <summary>
        /// Key: PPM Bin Based Start m/z
        /// </summary>
        PpmBinBasedStartMz = 16,    // Only used when InstrumentClass is 1 (ppm bin-based)

        /// <summary>
        /// Key: PPM Bin Base End m/z
        /// </summary>
        PpmBinBasedEndMz = 17,      // Only used when InstrumentClass is 1 (ppm bin-based)

        /// <summary>
        /// Key: Drift tube length in centimeters (for IMS)
        /// </summary>
        DriftTubeLength = 18,       // Only used for IMS

        /// <summary>
        /// Key: Drift Gas (for IMS)
        /// </summary>
        DriftGas = 19,
    }

    /// <summary>
    /// Instrument Class types
    /// </summary>
    public enum InstrumentClassType
    {
        /// <summary>
        /// TOF-based instrument
        /// </summary>
        TOF = 0,

        /// <summary>
        /// PPM bin based instrument
        /// </summary>
        PpmBinBased = 1
    }

    #endregion

    /// <summary>
    /// Global parameters
    /// </summary>
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
        public dynamic Value { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramType">Frame parameter definition</param>
        /// <param name="value">Parameter value</param>
        // ReSharper disable once UnusedMember.Global
        public GlobalParam(GlobalParamKeyType paramType, dynamic value)
        {
            InitializeByType(paramType);
            Value = value;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramType">Frame parameter definition</param>
        /// <param name="value">Parameter value</param>
        public GlobalParam(GlobalParamKeyType paramType, string value)
        {
            InitializeByType(paramType);
            Value = FrameParamUtilities.ConvertStringToDynamic(DataType, value);

            if (Value == null)
            {
                throw new InvalidCastException(
                    string.Format("GlobalParam constructor could not convert value of '{0}' for global parameter {1} to {2}",
                                  value, paramType, DataType));
            }
        }

        /// <summary>
        /// Customized ToString()
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (Value == null)
                return ParamType + " (" + DataType + ")";

            if (ParamType == GlobalParamKeyType.DateStarted && Value is DateTime)
            {
                // Make sure the date is in the standard format expected by Proteowizard
                // Proteowizard requires that it have AM/PM at the end

                return Value.ToString("M/d/yyyy h:mm:ss tt");
            }

            return Value.ToString();
        }

        /// <summary>
        /// Initialize this global parameter using the param type enum value
        /// </summary>
        /// <param name="paramType">Param key type enum</param>
        private void InitializeByType(GlobalParamKeyType paramType)
        {
            ParamType = paramType;

            var dataType = GlobalParamUtilities.GetGlobalParamKeyDataType(paramType);

            switch (paramType)
            {

                case GlobalParamKeyType.InstrumentName:
                    InitializeByType("InstrumentName", dataType, "Instrument name");
                    break;

                case GlobalParamKeyType.DateStarted:
                    // Format has traditionally been M/d/yyyy hh:mm:ss tt
                    // For example, 6/4/2014 12:56:44 PM
                    InitializeByType("DateStarted", dataType, "Time that the data acquisition started");
                    break;

                case GlobalParamKeyType.NumFrames:
                    InitializeByType("NumFrames", dataType, "Number of frames in the dataset");
                    break;

                case GlobalParamKeyType.TimeOffset:
                    InitializeByType("TimeOffset", dataType, "Time offset from 0 (in nanoseconds). All bin numbers must be offset by this amount");
                    break;

                case GlobalParamKeyType.BinWidth:
                    InitializeByType("BinWidth", dataType, "Width of TOF bins (in ns)");
                    break;

                case GlobalParamKeyType.Bins:
                    InitializeByType("Bins", dataType, "Total number of TOF bins in frame");
                    break;

                case GlobalParamKeyType.TOFCorrectionTime:
                    InitializeByType("TOFCorrectionTime", dataType, "TOF correction time");
                    break;

                case GlobalParamKeyType.TOFIntensityType:
                    InitializeByType("TOFIntensityType", dataType, "Data type of intensity in each TOF record (ADC is int, TDC is short, FOLDED is float)");
                    break;

                case GlobalParamKeyType.DatasetType:
                    InitializeByType("DatasetType", dataType, "Type of dataset (HMS, HMSn, or HMS-HMSn)");
                    break;

                case GlobalParamKeyType.PrescanTOFPulses:
                    InitializeByType("PrescanTOFPulses", dataType, "Prescan TOF pulses; this tracks the maximum scan number in any frame");
                    break;

                case GlobalParamKeyType.PrescanAccumulations:
                    InitializeByType("PrescanAccumulations", dataType, "Number of prescan accumulations");
                    break;

                case GlobalParamKeyType.PrescanTICThreshold:
                    InitializeByType("PrescanTICThreshold", dataType, "Prescan TIC threshold");
                    break;

                case GlobalParamKeyType.PrescanContinuous:
                    InitializeByType("PrescanContinuous", dataType, "Prescan Continuous flag (0 is false, 1 is true)");
                    break;

                case GlobalParamKeyType.PrescanProfile:
                    InitializeByType("PrescanProfile", dataType, "Profile used when PrescanContinuous is 1");
                    break;

                case GlobalParamKeyType.InstrumentClass:
                    InitializeByType("InstrumentClass", dataType, "Instrument class (0 for TOF, 1 for ppm bin-based)");
                    break;

                case GlobalParamKeyType.PpmBinBasedStartMz:
                    InitializeByType("PpmBinBasedStartMz", dataType, "Starting m/z value for ppm bin-based mode");
                    break;

                case GlobalParamKeyType.PpmBinBasedEndMz:
                    InitializeByType("PpmBinBasedEndMz", dataType, "Ending m/z value for ppm bin-based mode");
                    break;

                case GlobalParamKeyType.DriftTubeLength:
                    InitializeByType("DriftTubeLength", dataType, "IMS Drift tube length in centimeters");
                    break;

                case GlobalParamKeyType.DriftGas:
                    InitializeByType("DriftGas", dataType, "Drift gas (i.e., N2)");
                    break;

                case GlobalParamKeyType.Unknown:
                    throw new ArgumentOutOfRangeException(nameof(paramType), "Cannot initialize a global parameter of type Unknown: " + (int)paramType);

                default:
                    throw new ArgumentOutOfRangeException(nameof(paramType), "Unrecognized global param enum for paramType: " + (int)paramType);
            }

        }

        private void InitializeByType(string name, Type dataType, string description)
        {
            Name = name;
            DataType = dataType;
            Description = description;
        }

    }
}
