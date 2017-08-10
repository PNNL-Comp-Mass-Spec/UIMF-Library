using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    #region Frame Parameter Enum

    /// <summary>
    /// Known frame parameters
    /// </summary>
    public enum FrameParamKeyType
    {
        /// <summary>
        /// Unknown frame parameter key
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// Key: Frame start time
        /// </summary>
        StartTimeMinutes = 1,

        /// <summary>
        /// Key: Duration of frame
        /// </summary>
        DurationSeconds = 2,

        /// <summary>
        /// Key: Number of accumulations in frame
        /// </summary>
        Accumulations = 3,

        /// <summary>
        /// Key: FrameType
        /// </summary>
        FrameType = 4,

        /// <summary>
        /// Key: Decoded
        /// </summary>
        Decoded = 5,

        /// <summary>
        /// Key: Calibration done
        /// </summary>
        CalibrationDone = 6,

        /// <summary>
        /// Key: Scans
        /// </summary>
        Scans = 7,

        /// <summary>
        /// Key: Multiplexing Encoding sequence
        /// </summary>
        MultiplexingEncodingSequence = 8,        // Previously called IMFProfile

        /// <summary>
        /// Key: Multiplexing bit order
        /// </summary>
        MPBitOrder = 9,

        /// <summary>
        /// Key: TOF Losses
        /// </summary>
        TOFLosses = 10,

        /// <summary>
        /// Key: Average TOF length
        /// </summary>
        AverageTOFLength = 11,

        /// <summary>
        /// Key: Calibration Slope
        /// </summary>
        CalibrationSlope = 12,

        /// <summary>
        /// Key: Calibration Intercept
        /// </summary>
        CalibrationIntercept = 13,

        /// <summary>
        /// Key: Mass Calibration Coefficient: a2
        /// </summary>
        MassCalibrationCoefficienta2 = 14,

        /// <summary>
        /// Key: Mass Calibration Coefficient: b2
        /// </summary>
        MassCalibrationCoefficientb2 = 15,

        /// <summary>
        /// Key: Mass Calibration Coefficient: c2
        /// </summary>
        MassCalibrationCoefficientc2 = 16,

        /// <summary>
        /// Key: Mass Calibration Coefficient: d2
        /// </summary>
        MassCalibrationCoefficientd2 = 17,

        /// <summary>
        /// Key: Mass Calibration Coefficient: e2
        /// </summary>
        MassCalibrationCoefficiente2 = 18,

        /// <summary>
        /// Key: Mass Calibration Coefficient: f2
        /// </summary>
        MassCalibrationCoefficientf2 = 19,

        /// <summary>
        /// Key: Ambient temperature, in Celsius
        /// </summary>
        AmbientTemperature = 20,

        /// <summary>
        /// Key: Voltage High Voltage Rack 1
        /// </summary>
        VoltHVRack1 = 21,

        /// <summary>
        /// Key: Voltage High Voltage Rack 2
        /// </summary>
        VoltHVRack2 = 22,

        /// <summary>
        /// Key: Voltage High Voltage Rack 3
        /// </summary>
        VoltHVRack3 = 23,

        /// <summary>
        /// Key: Voltage High Voltage Rack 4
        /// </summary>
        VoltHVRack4 = 24,

        /// <summary>
        /// Key: Voltage Cap Inlet
        /// </summary>
        VoltCapInlet = 25,

        /// <summary>
        /// Key: Voltage Entrance HPF In
        /// </summary>
        VoltEntranceHPFIn = 26,

        /// <summary>
        /// Key: Voltage Entrance HPF Out
        /// </summary>
        VoltEntranceHPFOut = 27,

        /// <summary>
        /// Key: Voltage Entrance CondLmt
        /// </summary>
        VoltEntranceCondLmt = 28,

        /// <summary>
        /// Key: Voltage Trap Out
        /// </summary>
        VoltTrapOut = 29,

        /// <summary>
        /// Key: Voltage Trap In
        /// </summary>
        VoltTrapIn = 30,

        /// <summary>
        /// Key: Voltage Jet Dist
        /// </summary>
        VoltJetDist = 31,

        /// <summary>
        /// Key: Voltage Quad 1
        /// </summary>
        VoltQuad1 = 32,

        /// <summary>
        /// Key: Voltage Cond 1
        /// </summary>
        VoltCond1 = 33,

        /// <summary>
        /// Key: Voltage Quad 2
        /// </summary>
        VoltQuad2 = 34,

        /// <summary>
        /// Key: Voltage Cond 2
        /// </summary>
        VoltCond2 = 35,

        /// <summary>
        /// Key: Voltage IMS Out
        /// </summary>
        VoltIMSOut = 36,

        /// <summary>
        /// Key: Voltage Exit HPF In
        /// </summary>
        VoltExitHPFIn = 37,

        /// <summary>
        /// Key: Voltage Exit HPF Out
        /// </summary>
        VoltExitHPFOut = 38,

        /// <summary>
        /// Key: Voltage Exit CondLmt
        /// </summary>
        VoltExitCondLmt = 39,

        /// <summary>
        /// Key: Pressure Front
        /// </summary>
        PressureFront = 40,

        /// <summary>
        /// Key: Pressure Back
        /// </summary>
        PressureBack = 41,

        /// <summary>
        /// Key: High Pressure Funnel Pressure
        /// </summary>
        HighPressureFunnelPressure = 42,

        /// <summary>
        /// Key: Ion Funnel Trap Pressure
        /// </summary>
        IonFunnelTrapPressure = 43,

        /// <summary>
        /// Key: Rear Ion Funnel Pressure
        /// </summary>
        RearIonFunnelPressure = 44,

        /// <summary>
        /// Key: Quadrupole Pressure
        /// </summary>
        QuadrupolePressure = 45,

        /// <summary>
        /// Key: ESI Voltage
        /// </summary>
        ESIVoltage = 46,

        /// <summary>
        /// Key: Float Voltage
        /// </summary>
        FloatVoltage = 47,

        /// <summary>
        /// Key: Fragmentation Profile
        /// </summary>
        FragmentationProfile = 48,

        /// <summary>
        /// Key: Scan Number First
        /// </summary>
        ScanNumFirst = 49,

        /// <summary>
        /// Key: Scan Number Last
        /// </summary>
        ScanNumLast = 50,

        /// <summary>
        /// Key: Pressure Units
        /// </summary>
        PressureUnits = 51,

        /// <summary>
        /// Key: Temperature of the drift tube, in Celsius
        /// </summary>
        DriftTubeTemperature = 52,

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
        public FrameParamKeyType ParamType { get; }

        /// <summary>
        /// Parameter Name
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// .NET data type
        /// </summary>
        public Type DataType { get; }

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
