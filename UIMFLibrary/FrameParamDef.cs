using System;

namespace UIMFLibrary
{
    #region Frame Parameter Enum

    /// <summary>
    /// Known frame parameters
    /// </summary>
    public enum FrameParamKeyType
    {
        Unknown = 0,
        StartTimeMinutes = 1,
        DurationSeconds = 2,
        Accumulations = 3,
        FrameType = 4,
        Decoded = 5,
        CalibrationDone = 6,
        Scans = 7,
        MultiplexingEncodingSequence = 8,
        MPBitOrder = 9,
        TOFLosses = 10,
        AverageTOFLength = 11,
        CalibrationSlope = 12,
        CalibrationIntercept = 13,
        MassCalibrationCoefficienta2 = 14,
        MassCalibrationCoefficientb2 = 15,
        MassCalibrationCoefficientc2 = 16,
        MassCalibrationCoefficientd2 = 17,
        MassCalibrationCoefficiente2 = 18,
        MassCalibrationCoefficientf2 = 19,
        AmbientTemperature = 20,
        VoltHVRack1 = 21,
        VoltHVRack2 = 22,
        VoltHVRack3 = 23,
        VoltHVRack4 = 24,
        VoltCapInlet = 25,
        VoltEntranceHPFIn = 26,
        VoltEntranceHPFOut = 27,
        VoltEntranceCondLmt = 28,
        VoltTrapOut = 29,
        VoltTrapIn = 30,
        VoltJetDist = 31,
        VoltQuad1 = 32,
        VoltCond1 = 33,
        VoltQuad2 = 34,
        VoltCond2 = 35,
        VoltIMSOut = 36,
        VoltExitHPFIn = 37,
        VoltExitHPFOut = 38,
        VoltExitCondLmt = 39,
        PressureFront = 40,
        PressureBack = 41,
        HighPressureFunnelPressure = 42,
        IonFunnelTrapPressure = 43,
        RearIonFunnelPressure = 44,
        QuadrupolePressure = 45,
        ESIVoltage = 46,
        FloatVoltage = 47,
        FragmentationProfile = 48,
        ScanNumFirst= 49,
        ScanNumLast = 50,
        PressureUnits = 51
    }

    #endregion

    /// <summary>
    /// Frame parameter definition
    /// </summary>
    public class FrameParamDef
    {

        #region Properties

        /// <summary>
        /// Parameter Type
        /// </summary>
        public FrameParamKeyType ParamType { get; private set; }

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
    
        #endregion
     
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramType">Strongly typed enum of the new parameter</param>
        public FrameParamDef(FrameParamKeyType paramType)
        {
            var paramDef = FrameParamUtilities.GetParamDefByType(paramType);

            ParamType = paramDef.ParamType;
            Name = paramDef.Name;
            DataType = paramDef.DataType;
            Description = paramDef.Description;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramType">Strongly typed enum of the new parameter</param>
        /// <param name="name">Parameter name</param>
        /// <param name="dataType">Parameter .NET data type (as a string)</param>
        /// <param name="description">Parameter description</param>
        /// <remarks>Does not verify that paramID is a valid member of FrameParamKeyType</remarks>
        public FrameParamDef(FrameParamKeyType paramType, string name, string dataType, string description = "")
        {
            ParamType = paramType;
            Name = name;

            try
            {
                DataType = Type.GetType(dataType);

                if (DataType == null)
                {
                    dataType = GlobalParamUtilities.GetDataTypeFromAlias(dataType);

                    DataType = Type.GetType(dataType) ?? typeof(object);
                }
            }
            catch
            {
                DataType = typeof(object);
            }

            Description = description;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramType">Strongly typed enum of the new parameter</param>
        /// <param name="name">Parameter name</param>
        /// <param name="dataType">Parameter .NET data type (as a Type)</param>
        /// <param name="description">Parameter description</param>
        /// <remarks>Does not verify that paramID is a valid member of FrameParamKeyType</remarks>
        public FrameParamDef(FrameParamKeyType paramType, string name, Type dataType, string description = "")
        {
            ParamType = paramType;
            Name = name;
            DataType = dataType;
            Description = description;
        }

        /// <summary>
        /// Clone this frame parameter definition (deep copy)
        /// </summary>
        /// <returns></returns>
        public FrameParamDef CopyTo()
        {
            var paramDefCopy = new FrameParamDef(ParamType, Name, DataType, Description);
            return paramDefCopy;
        }

        /// <summary>
        /// Customized ToString()
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ParamType + " (" + DataType + ")";
        }

    }
}
