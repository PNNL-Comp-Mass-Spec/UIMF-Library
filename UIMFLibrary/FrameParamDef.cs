using System;

namespace UIMFLibrary
{
    #region Enums

    /// <summary>
    /// Known param key types
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
        MassErrorCoefficienta2 = 14,
        MassErrorCoefficientb2 = 15,
        MassErrorCoefficientc2 = 16,
        MassErrorCoefficientd2 = 17,
        MassErrorCoefficiente2 = 18,
        MassErrorCoefficientf2 = 19,
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
        FragmentationProfile = 48
    }

    #endregion

    /// <summary>
    /// Frame parameter metadata class
    /// </summary>
    public class FrameParamDef : ICloneable
    {

        #region Properties

        /// <summary>
        /// Parameter ID
        /// </summary>
        public FrameParamKeyType ID { get; private set; }

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
        /// Value defined for this parameter
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Strongly typed enum of the parameter
        /// </summary>
        /// <remarks>Returns FrameParamKeyType.Unknown if not defined in enum FrameParamKeyType</remarks>
        public FrameParamKeyType ParamType
        {
            get
            {
                if (!Enum.IsDefined(typeof (FrameParamKeyType), ID))
                    return FrameParamKeyType.Unknown;

                return (FrameParamKeyType)ID;
            }
        }

        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameParamDef"/> class. 
        /// </summary>
        /// <param name="paramID">Parameter ID</param>
        /// <param name="paramName">Parameter name</param>
        /// <param name="paramDataType">Parameter .NET data type</param>
        /// <param name="paramDescription">Parameter description</param>
        /// <remarks>Does not verify that paramID is a valid member of FrameParamKeyType</remarks>
        public FrameParamDef(FrameParamKeyType paramID, string paramName, string paramDataType, string paramDescription = "")
        {
            ID = paramID;
            Name = paramName;

            try
            {
                DataType = Type.GetType(paramDataType);
            }
            catch
            {
                DataType = typeof(object);
            }

            Description = paramDescription;
            Value = null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameParamDef"/> class. 
        /// </summary>
        /// <param name="paramType">Strongly typed enum of the new parameter</param>
        public FrameParamDef(FrameParamKeyType paramType)
        {
            var param = FrameParamUtilities.GetParamDefByType(paramType);

            ID = (int)param.ParamType;
            Name = param.Name;
            DataType = param.DataType;
            Description = param.Description;
            Value = null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameParamDef"/> class. 
        /// </summary>
        /// <param name="paramType">Strongly typed enum of the new parameter</param>
        /// <param name="paramName">Parameter name</param>
        /// <param name="paramDataType">Parameter .NET data type</param>
        /// <param name="paramDescription">Parameter description</param>
        /// <remarks>Does not verify that paramID is a valid member of FrameParamKeyType</remarks>
        public FrameParamDef(FrameParamKeyType paramType, string paramName, string paramDataType,
                          string paramDescription = "")
        {
            ID = paramType;
            Name = paramName;
            DataType = paramDataType;
            Description = paramDescription;
        }

        /// <summary>
        /// Override ToString()
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ID + ": " + ParamType + " (" + DataType + ")";
        }

    }
}
