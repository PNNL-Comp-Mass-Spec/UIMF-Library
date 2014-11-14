using System;
using System.Collections.Generic;
using System.Linq;

namespace UIMFLibrary
{
    public static class FrameParamUtilities
    {       
        /// <summary>
        /// Create a frame parameter dictionary using a FrameParameters class instance
        /// </summary>
        /// <param name="legacyFrameParams"></param>
        /// <returns></returns>
        public static Dictionary<FrameParamDef, string> ConvertFrameParameters(this FrameParamDef fp, FrameParameters legacyFrameParams)
        {
            //xxx this needs to be coded

            throw new NotImplementedException();
        }

        /// <summary>
        /// Resolve FrameParam Key Type using the parameter id integer value
        /// </summary>
        /// <param name="paramID"></param>
        /// <returns>Specific FrameParamKeyType enum, or FrameParamKeyType.Unknown</returns>
        public static FrameParamKeyType GetParamTypeByID(int paramID)
        {
            if (Enum.IsDefined(typeof(FrameParamKeyType), paramID))
                return (FrameParamKeyType)paramID;

            return FrameParamKeyType.Unknown;
        }

        /// <summary>
        /// Resolve FrameParam Key Type using the parameter name
        /// </summary>
        /// <param name="paramName"></param>
        /// <returns>Specific FrameParamKeyType enum, or FrameParamKeyType.Unknown</returns>
        public static FrameParamKeyType GetParamTypeByName(string paramName)
        {
            int iteration = 0;

            while (iteration < 2)
            {

                try
                {
                    // Note that this conversion works for both the names in the FrameParamKeyType enum and for the integer values
                    // See MSDN's "Enum.Parse Method" page at http://msdn.microsoft.com/en-us/library/essfb559.aspx
                    bool ignoreCase = iteration > 0;
                    var paramType = (FrameParamKeyType)Enum.Parse(typeof (FrameParamKeyType), paramName, ignoreCase);
                    if (Enum.IsDefined(typeof (FrameParamKeyType), paramType) | paramType.ToString().Contains(","))
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

            // Auto-conversion failed
            // Look for a synonym
            switch (paramName)
            {
                case "StartTime":
                    return FrameParamKeyType.StartTimeMinutes;

                case "Duration":
                    return FrameParamKeyType.DurationSeconds;

                case "IMFProfile":
                    return FrameParamKeyType.MultiplexingEncodingSequence;

                case "a2":
                    return FrameParamKeyType.MassErrorCoefficienta2;
                case "b2":
                    return FrameParamKeyType.MassErrorCoefficientb2;
                case "c2":
                    return FrameParamKeyType.MassErrorCoefficientc2;
                case "d2":
                    return FrameParamKeyType.MassErrorCoefficientd2;
                case "e2":
                    return FrameParamKeyType.MassErrorCoefficiente2;
                case "f2":
                    return FrameParamKeyType.MassErrorCoefficientf2;

                case "Temperature":
                    return FrameParamKeyType.AmbientTemperature;

                case "voltEntranceIFTIn":
                    return FrameParamKeyType.VoltEntranceHPFIn;
                case "voltEntranceIFTOut":
                    return FrameParamKeyType.VoltEntranceHPFOut;

                case "voltExitIFTIn":
                    return FrameParamKeyType.VoltExitHPFIn;
                case "voltExitIFTOut":
                    return FrameParamKeyType.VoltExitHPFOut;

                default:
                    return FrameParamKeyType.Unknown;
            }

        }

        /// <summary>
        /// Obtain a frame parameter definition instance given a parameter name
        /// </summary>
        /// <param name="paramName">Param key name</param>
        /// <returns>FrameParam instance</returns>
        public static FrameParamDef GetParamDefByName(string paramName)
        {
            if (String.IsNullOrWhiteSpace(paramName))
                throw new ArgumentOutOfRangeException("paramName", "paramName is empty");

            var paramType = GetParamTypeByName(paramName);

            if (paramType == FrameParamKeyType.Unknown)
                throw new ArgumentOutOfRangeException("paramName", "unknown value for paramName: " + paramName);

            return GetParamDefByType(paramType);
        }

        /// <summary>
        /// Obtain a frame parameter definition instance given a parameter key type enum value
        /// </summary>
        /// <param name="paramType">Param key type enum</param>
        /// <returns>FrameParam instance</returns>
        /// <remarks>Will include the official parameter name, description, and data type for the given param key</remarks>
        public static FrameParamDef GetParamDefByType(FrameParamKeyType paramType)
        {
            switch (paramType)
            {

                case FrameParamKeyType.StartTimeMinutes:
                    return new FrameParamDef(FrameParamKeyType.StartTimeMinutes, "StartTime", "float",
                                          "Start time of frame, in minutes");

                case FrameParamKeyType.DurationSeconds:
                    return new FrameParamDef(FrameParamKeyType.DurationSeconds,
                                          FrameParamKeyType.DurationSeconds.ToString(), "float",
                                          "Frame duration, in seconds");

                case FrameParamKeyType.Accumulations:
                    return new FrameParamDef(FrameParamKeyType.Accumulations, FrameParamKeyType.Accumulations.ToString(),
                                          "int",
                                          "Number of collected and summed acquisitions in a frame");

                case FrameParamKeyType.FrameType:
                    // Allowed values defined by DataReader.FrameType
                    return new FrameParamDef(FrameParamKeyType.FrameType, FrameParamKeyType.FrameType.ToString(), "int",
                                          "Frame Type: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan");

                case FrameParamKeyType.Decoded:
                    // Allowed values are 0 or 1
                    return new FrameParamDef(FrameParamKeyType.Decoded, FrameParamKeyType.Decoded.ToString(), "int",
                                          "Tracks whether frame has been decoded: 0 for non-multiplexed or encoded; 1 if decoded");

                case FrameParamKeyType.CalibrationDone:
                    // Allowed values are 0 or 1, though -1 was used in the past instead of 0
                    return new FrameParamDef(FrameParamKeyType.CalibrationDone,
                                          FrameParamKeyType.CalibrationDone.ToString(), "int",
                                          "Tracks whether frame has been calibrated: 1 if calibrated");

                case FrameParamKeyType.Scans:
                    return new FrameParamDef(FrameParamKeyType.Scans, FrameParamKeyType.Scans.ToString(), "int",
                                          "Number of TOF scans in a frame");

                case FrameParamKeyType.MultiplexingEncodingSequence:
                    return new FrameParamDef(FrameParamKeyType.MultiplexingEncodingSequence,
                                          FrameParamKeyType.MultiplexingEncodingSequence.ToString(), "string",
                                          "The name of the sequence used to encode the data when acquiring multiplexed data");

                case FrameParamKeyType.MPBitOrder:
                    return new FrameParamDef(FrameParamKeyType.MPBitOrder, FrameParamKeyType.MPBitOrder.ToString(), "int",
                                          "Multiplexing bit order; Determines size of the bit sequence");

                case FrameParamKeyType.TOFLosses:
                    return new FrameParamDef(FrameParamKeyType.TOFLosses, FrameParamKeyType.TOFLosses.ToString(), "int",
                                          "Number of TOF Losses (lost/skipped scans due to I/O problems)");

                case FrameParamKeyType.AverageTOFLength:
                    return new FrameParamDef(FrameParamKeyType.AverageTOFLength,
                                          FrameParamKeyType.AverageTOFLength.ToString(), "float",
                                          "Average time between TOF trigger pulses, in nanoseconds");

                case FrameParamKeyType.CalibrationSlope:
                    return new FrameParamDef(FrameParamKeyType.CalibrationSlope,
                                          FrameParamKeyType.CalibrationSlope.ToString(), "double",
                                          "Calibration slope, k0");

                case FrameParamKeyType.CalibrationIntercept:
                    return new FrameParamDef(FrameParamKeyType.CalibrationIntercept,
                                          FrameParamKeyType.CalibrationIntercept.ToString(), "double",
                                          "Calibration intercept, t0");

                case FrameParamKeyType.MassErrorCoefficienta2:
                    return new FrameParamDef(FrameParamKeyType.MassErrorCoefficienta2,
                                          FrameParamKeyType.MassErrorCoefficienta2.ToString(), "double",
                                          "a2 parameter for residual mass error correction; ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11");

                case FrameParamKeyType.MassErrorCoefficientb2:
                    return new FrameParamDef(FrameParamKeyType.MassErrorCoefficientb2,
                                          FrameParamKeyType.MassErrorCoefficientb2.ToString(), "double",
                                          "b2 parameter for residual mass error correction");

                case FrameParamKeyType.MassErrorCoefficientc2:
                    return new FrameParamDef(FrameParamKeyType.MassErrorCoefficientc2,
                                          FrameParamKeyType.MassErrorCoefficientc2.ToString(), "double",
                                          "c2 parameter for residual mass error correction");

                case FrameParamKeyType.MassErrorCoefficientd2:
                    return new FrameParamDef(FrameParamKeyType.MassErrorCoefficientd2,
                                          FrameParamKeyType.MassErrorCoefficientd2.ToString(), "double",
                                          "db2 parameter for residual mass error correction");

                case FrameParamKeyType.MassErrorCoefficiente2:
                    return new FrameParamDef(FrameParamKeyType.MassErrorCoefficiente2,
                                          FrameParamKeyType.MassErrorCoefficiente2.ToString(), "double",
                                          "e2 parameter for residual mass error correction");

                case FrameParamKeyType.MassErrorCoefficientf2:
                    return new FrameParamDef(FrameParamKeyType.MassErrorCoefficientf2,
                                          FrameParamKeyType.MassErrorCoefficientf2.ToString(), "double",
                                          "f2 parameter for residual mass error correction");

                case FrameParamKeyType.AmbientTemperature:
                    return new FrameParamDef(FrameParamKeyType.AmbientTemperature,
                                          FrameParamKeyType.AmbientTemperature.ToString(), "float",
                                          "Ambient temperature, in Celcius");

                case FrameParamKeyType.VoltHVRack1:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack1, FrameParamKeyType.VoltHVRack1.ToString(),
                                          "float",
                                          "Volt hv rack 1");

                case FrameParamKeyType.VoltHVRack2:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack2, FrameParamKeyType.VoltHVRack2.ToString(),
                                          "float",
                                          "Volt hv rack 2");

                case FrameParamKeyType.VoltHVRack3:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack3, FrameParamKeyType.VoltHVRack3.ToString(),
                                          "float",
                                          "Volt hv rack 3");

                case FrameParamKeyType.VoltHVRack4:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack4, FrameParamKeyType.VoltHVRack4.ToString(),
                                          "float",
                                          "Volt hv rack 4");

                case FrameParamKeyType.VoltCapInlet:
                    return new FrameParamDef(FrameParamKeyType.VoltCapInlet, FrameParamKeyType.VoltCapInlet.ToString(),
                                          "float",
                                          "Capillary Inlet Voltage");

                case FrameParamKeyType.VoltEntranceHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFIn,
                                          FrameParamKeyType.VoltEntranceHPFIn.ToString(), "float",
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltEntranceHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFOut,
                                          FrameParamKeyType.VoltEntranceHPFOut.ToString(), "float",
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltEntranceCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceCondLmt,
                                          FrameParamKeyType.VoltEntranceCondLmt.ToString(), "float",
                                          "Entrance Cond Limit Voltage");

                case FrameParamKeyType.VoltTrapOut:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapOut, FrameParamKeyType.VoltTrapOut.ToString(),
                                          "float",
                                          "Trap Out Voltage");

                case FrameParamKeyType.VoltTrapIn:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapIn, FrameParamKeyType.VoltTrapIn.ToString(), "float",
                                          "Trap In Voltage");

                case FrameParamKeyType.VoltJetDist:
                    return new FrameParamDef(FrameParamKeyType.VoltJetDist, FrameParamKeyType.VoltJetDist.ToString(),
                                          "float",
                                          "Jet Disruptor Voltage");

                case FrameParamKeyType.VoltQuad1:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad1, FrameParamKeyType.VoltQuad1.ToString(), "float",
                                          "Fragmentation Quadrupole Voltage 1");

                case FrameParamKeyType.VoltCond1:
                    return new FrameParamDef(FrameParamKeyType.VoltCond1, FrameParamKeyType.VoltCond1.ToString(), "float",
                                          "Fragmentation Conductance Voltage 1");

                case FrameParamKeyType.VoltQuad2:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad2, FrameParamKeyType.VoltQuad2.ToString(), "float",
                                          "Fragmentation Quadrupole Voltage 2");

                case FrameParamKeyType.VoltCond2:
                    return new FrameParamDef(FrameParamKeyType.VoltCond2, FrameParamKeyType.VoltCond2.ToString(), "float",
                                          "Fragmentation Conductance Voltage 2");

                case FrameParamKeyType.VoltIMSOut:
                    return new FrameParamDef(FrameParamKeyType.VoltIMSOut, FrameParamKeyType.VoltIMSOut.ToString(), "float",
                                          "IMS Out Voltage");

                case FrameParamKeyType.VoltExitHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFIn, FrameParamKeyType.VoltExitHPFIn.ToString(),
                                          "float",
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltExitHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFOut, FrameParamKeyType.VoltExitHPFOut.ToString(),
                                          "float",
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltExitCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltExitCondLmt,
                                          FrameParamKeyType.VoltExitCondLmt.ToString(), "float",
                                          "Exit Cond Limit Voltage");

                case FrameParamKeyType.PressureFront:
                    return new FrameParamDef(FrameParamKeyType.PressureFront, FrameParamKeyType.PressureFront.ToString(),
                                          "float",
                                          "Pressure at front of Drift Tube");

                case FrameParamKeyType.PressureBack:
                    return new FrameParamDef(FrameParamKeyType.PressureBack, FrameParamKeyType.PressureBack.ToString(),
                                          "float",
                                          "Pressure at back of Drift Tube");

                case FrameParamKeyType.HighPressureFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.HighPressureFunnelPressure,
                                          FrameParamKeyType.HighPressureFunnelPressure.ToString(), "float",
                                          "High pressure funnel pressure");

                case FrameParamKeyType.IonFunnelTrapPressure:
                    return new FrameParamDef(FrameParamKeyType.IonFunnelTrapPressure,
                                          FrameParamKeyType.IonFunnelTrapPressure.ToString(), "float",
                                          "Ion funnel trap pressure");

                case FrameParamKeyType.RearIonFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.RearIonFunnelPressure,
                                          FrameParamKeyType.RearIonFunnelPressure.ToString(), "float",
                                          "Rear ion funnel pressure");

                case FrameParamKeyType.QuadrupolePressure:
                    return new FrameParamDef(FrameParamKeyType.QuadrupolePressure,
                                          FrameParamKeyType.QuadrupolePressure.ToString(), "float",
                                          "Quadrupole pressure");

                case FrameParamKeyType.ESIVoltage:
                    return new FrameParamDef(FrameParamKeyType.ESIVoltage, FrameParamKeyType.ESIVoltage.ToString(), "float",
                                          "ESI voltage");

                case FrameParamKeyType.FloatVoltage:
                    return new FrameParamDef(FrameParamKeyType.FloatVoltage, FrameParamKeyType.FloatVoltage.ToString(),
                                          "float",
                                          "Float voltage");

                case FrameParamKeyType.FragmentationProfile:
                    return new FrameParamDef(FrameParamKeyType.FragmentationProfile,
                                          FrameParamKeyType.FragmentationProfile.ToString(), "string",
                                          "Voltage profile used in fragmentation (Base 64 encoded array of doubles)");

                default:
                    throw new ArgumentOutOfRangeException("paramType", "Unrecognized enum for paramType: " + (int)paramType);
            }

        }



        public static double TryGetFrameParam(
            Dictionary<FrameParamDef, string> frameParams,
            FrameParamKeyType paramType,
            double defaultValue)
        {
            bool paramNotDefined;
            return TryGetFrameParam(frameParams, paramType, defaultValue, out paramNotDefined);
        }

        public static double TryGetFrameParam(
            Dictionary<FrameParamDef, string> frameParams,
            FrameParamKeyType paramType,
            double defaultValue,
            out bool paramNotDefined)
        {
            string paramValue = string.Empty;
            paramNotDefined = true;

            foreach (var item in frameParams.Where(item => item.Key.ParamType == paramType))
            {
                paramValue = item.Value;
                paramNotDefined = false;
                break;
            }

            if (paramNotDefined)
                return defaultValue;

            double result;
            if (double.TryParse(paramValue, out result))
                return result;

            return defaultValue;

        }

        public static int TryGetFrameParamInt32(
                    Dictionary<FrameParamDef, string> frameParams,
                    FrameParamKeyType paramType,
                    int defaultValue)
        {
            bool paramNotDefined;
            return TryGetFrameParamInt32(frameParams, paramType, defaultValue, out paramNotDefined);
        }

        public static int TryGetFrameParamInt32(
           Dictionary<FrameParamDef, string> frameParams,
           FrameParamKeyType paramType,
           int defaultValue,
           out bool paramNotDefined)
        {
            string paramValue = string.Empty;
            paramNotDefined = true;

            foreach (var item in frameParams.Where(item => item.Key.ParamType == paramType))
            {
                paramValue = item.Value;
                paramNotDefined = false;
                break;
            }

            if (paramNotDefined)
                return defaultValue;

            int result;
            if (int.TryParse(paramValue, out result))
                return result;

            return defaultValue;

        }

    }

}
