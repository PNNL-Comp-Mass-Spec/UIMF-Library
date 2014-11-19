using System;
using System.Collections.Generic;
using System.Linq;

namespace UIMFLibrary
{
    /// <summary>
    /// Utility functions for working with frame parameters
    /// </summary>
    public static class FrameParamUtilities
    {
       
        /// <summary>
        /// Create a frame parameter dictionary using a FrameParameters class instance
        /// </summary>
        /// <param name="frameParameters"></param>
        /// <returns>Frame parameter dictionary</returns>
#pragma warning disable 612, 618
        public static Dictionary<FrameParamKeyType, string> ConvertFrameParameters(FrameParameters frameParameters)
#pragma warning restore 612, 618
        {
            var frameParams = new Dictionary<FrameParamKeyType, string>
            {
	            // Start time of frame, in minutes
	            {FrameParamKeyType.StartTimeMinutes, UIMFDataUtilities.FloatToString(frameParameters.StartTime)},
                
	            // Duration of frame, in seconds
	            {FrameParamKeyType.DurationSeconds, UIMFDataUtilities.FloatToString(frameParameters.Duration)},
                
	            // Number of collected and summed acquisitions in a frame 
	            {FrameParamKeyType.Accumulations, UIMFDataUtilities.IntToString(frameParameters.Accumulations)},
                
	            // Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
	            {FrameParamKeyType.FrameType, UIMFDataUtilities.IntToString((int)frameParameters.FrameType)},

                // Set to 1 after a frame has been decoded (added June 27, 2011)
                {FrameParamKeyType.Decoded, UIMFDataUtilities.IntToString(frameParameters.Decoded)},

                // Set to 1 after a frame has been calibrated
                {FrameParamKeyType.CalibrationDone, UIMFDataUtilities.IntToString(frameParameters.CalibrationDone)},

	            // Number of TOF scans
	            {FrameParamKeyType.Scans, UIMFDataUtilities.IntToString(frameParameters.Scans)},

	            // IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed
	            {FrameParamKeyType.MultiplexingEncodingSequence, frameParameters.IMFProfile},

	            // Original size of bit sequence
	            {FrameParamKeyType.MPBitOrder, UIMFDataUtilities.IntToString(frameParameters.MPBitOrder)},

                // Number of TOF Losses
	            {FrameParamKeyType.TOFLosses, UIMFDataUtilities.IntToString(frameParameters.TOFLosses)},
	        
                // Average time between TOF trigger pulses
                {FrameParamKeyType.AverageTOFLength, UIMFDataUtilities.FloatToString(frameParameters.AverageTOFLength)},

                // Calibration slope, k0
	            {FrameParamKeyType.CalibrationSlope, UIMFDataUtilities.DoubleToString(frameParameters.CalibrationSlope)},

                // Calibration intercept, t0
	            {FrameParamKeyType.CalibrationIntercept, UIMFDataUtilities.DoubleToString(frameParameters.CalibrationIntercept)}
	        };

            // These six parameters are coefficients for residual mass error correction      
            // ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11
            if (Math.Abs(frameParameters.a2) > Single.Epsilon ||
                Math.Abs(frameParameters.b2) > Single.Epsilon ||
                Math.Abs(frameParameters.c2) > Single.Epsilon ||
                Math.Abs(frameParameters.d2) > Single.Epsilon ||
                Math.Abs(frameParameters.e2) > Single.Epsilon ||
                Math.Abs(frameParameters.f2) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.MassErrorCoefficienta2, UIMFDataUtilities.DoubleToString(frameParameters.a2));
                frameParams.Add(FrameParamKeyType.MassErrorCoefficientb2, UIMFDataUtilities.DoubleToString(frameParameters.b2));
                frameParams.Add(FrameParamKeyType.MassErrorCoefficientc2, UIMFDataUtilities.DoubleToString(frameParameters.c2));
                frameParams.Add(FrameParamKeyType.MassErrorCoefficientd2, UIMFDataUtilities.DoubleToString(frameParameters.d2));
                frameParams.Add(FrameParamKeyType.MassErrorCoefficiente2, UIMFDataUtilities.DoubleToString(frameParameters.e2));
                frameParams.Add(FrameParamKeyType.MassErrorCoefficientf2, UIMFDataUtilities.DoubleToString(frameParameters.f2));
            }

            // Ambient temperature
            frameParams.Add(FrameParamKeyType.AmbientTemperature, UIMFDataUtilities.FloatToString(frameParameters.Temperature));

            // Voltage settings in the IMS system
            if (Math.Abs(frameParameters.voltHVRack1) > Single.Epsilon ||
                Math.Abs(frameParameters.voltHVRack2) > Single.Epsilon ||
                Math.Abs(frameParameters.voltHVRack3) > Single.Epsilon ||
                Math.Abs(frameParameters.voltHVRack4) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltHVRack1, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack1));
                frameParams.Add(FrameParamKeyType.VoltHVRack2, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack2));
                frameParams.Add(FrameParamKeyType.VoltHVRack3, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack3));
                frameParams.Add(FrameParamKeyType.VoltHVRack4, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack4));
            }

            // Capillary Inlet Voltage
            // HPF In Voltage
            // HPF Out Voltage
            // Cond Limit Voltage
            if (Math.Abs(frameParameters.voltEntranceHPFIn) > Single.Epsilon ||
                Math.Abs(frameParameters.voltEntranceHPFIn) > Single.Epsilon ||
                Math.Abs(frameParameters.voltEntranceHPFOut) > Single.Epsilon ||
                Math.Abs(frameParameters.voltEntranceCondLmt) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltCapInlet, UIMFDataUtilities.FloatToString(frameParameters.voltCapInlet));
                frameParams.Add(FrameParamKeyType.VoltEntranceHPFIn, UIMFDataUtilities.FloatToString(frameParameters.voltEntranceHPFIn));
                frameParams.Add(FrameParamKeyType.VoltEntranceHPFOut, UIMFDataUtilities.FloatToString(frameParameters.voltEntranceHPFOut));
                frameParams.Add(FrameParamKeyType.VoltEntranceCondLmt, UIMFDataUtilities.FloatToString(frameParameters.voltEntranceCondLmt));
            }

            // Trap Out Voltage
            // Trap In Voltage
            // Jet Disruptor Voltage
            if (Math.Abs(frameParameters.voltTrapOut) > Single.Epsilon ||
                Math.Abs(frameParameters.voltTrapIn) > Single.Epsilon ||
                Math.Abs(frameParameters.voltJetDist) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltTrapOut, UIMFDataUtilities.FloatToString(frameParameters.voltTrapOut));
                frameParams.Add(FrameParamKeyType.VoltTrapIn, UIMFDataUtilities.FloatToString(frameParameters.voltTrapIn));
                frameParams.Add(FrameParamKeyType.VoltJetDist, UIMFDataUtilities.FloatToString(frameParameters.voltJetDist));
            }

            // Fragmentation Quadrupole 1 Voltage
            // Fragmentation Conductance 1 Voltage
            if (Math.Abs(frameParameters.voltQuad1) > Single.Epsilon ||
                Math.Abs(frameParameters.voltCond1) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltQuad1, UIMFDataUtilities.FloatToString(frameParameters.voltQuad1));
                frameParams.Add(FrameParamKeyType.VoltCond1, UIMFDataUtilities.FloatToString(frameParameters.voltCond1));
            }

            // Fragmentation Quadrupole 2 Voltage
            // Fragmentation Conductance 2 Voltage
            if (Math.Abs(frameParameters.voltQuad2) > Single.Epsilon ||
                Math.Abs(frameParameters.voltCond2) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltQuad2, UIMFDataUtilities.FloatToString(frameParameters.voltQuad2));
                frameParams.Add(FrameParamKeyType.VoltCond2, UIMFDataUtilities.FloatToString(frameParameters.voltCond2));
            }

            // IMS Out Voltage
            // HPF In Voltage
            // HPF Out Voltage
            if (Math.Abs(frameParameters.voltIMSOut) > Single.Epsilon ||
                Math.Abs(frameParameters.voltExitHPFIn) > Single.Epsilon ||
                Math.Abs(frameParameters.voltExitHPFOut) > Single.Epsilon ||
                Math.Abs(frameParameters.voltExitCondLmt) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltIMSOut, UIMFDataUtilities.FloatToString(frameParameters.voltIMSOut));
                frameParams.Add(FrameParamKeyType.VoltExitHPFIn, UIMFDataUtilities.FloatToString(frameParameters.voltExitHPFIn));
                frameParams.Add(FrameParamKeyType.VoltExitHPFOut, UIMFDataUtilities.FloatToString(frameParameters.voltExitHPFOut));
                frameParams.Add(FrameParamKeyType.VoltExitCondLmt, UIMFDataUtilities.FloatToString(frameParameters.voltExitCondLmt));
            }

            // Pressure at front of Drift Tube
            // Pressure at back of Drift Tube
            if (Math.Abs(frameParameters.PressureFront) > Single.Epsilon ||
                Math.Abs(frameParameters.PressureBack) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.PressureFront, UIMFDataUtilities.FloatToString(frameParameters.PressureFront));
                frameParams.Add(FrameParamKeyType.PressureBack, UIMFDataUtilities.FloatToString(frameParameters.PressureBack));
            }

            // High pressure funnel pressure
            // Ion funnel trap pressure
            // Rear ion funnel pressure
            // Quadruple pressure
            if (Math.Abs(frameParameters.HighPressureFunnelPressure) > Single.Epsilon ||
                Math.Abs(frameParameters.IonFunnelTrapPressure) > Single.Epsilon ||
                Math.Abs(frameParameters.RearIonFunnelPressure) > Single.Epsilon ||
                Math.Abs(frameParameters.QuadrupolePressure) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.HighPressureFunnelPressure, UIMFDataUtilities.FloatToString(frameParameters.HighPressureFunnelPressure));
                frameParams.Add(FrameParamKeyType.IonFunnelTrapPressure, UIMFDataUtilities.FloatToString(frameParameters.IonFunnelTrapPressure));
                frameParams.Add(FrameParamKeyType.RearIonFunnelPressure, UIMFDataUtilities.FloatToString(frameParameters.RearIonFunnelPressure));
                frameParams.Add(FrameParamKeyType.QuadrupolePressure, UIMFDataUtilities.FloatToString(frameParameters.QuadrupolePressure));
            }

            // ESI Voltage
            if (Math.Abs(frameParameters.ESIVoltage) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.ESIVoltage, UIMFDataUtilities.FloatToString(frameParameters.ESIVoltage));
            }

            // Float Voltage
            if (Math.Abs(frameParameters.FloatVoltage) > Single.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.FloatVoltage, UIMFDataUtilities.FloatToString(frameParameters.FloatVoltage));
            }


            // Voltage profile used in fragmentation
            // Legacy parameter, likely never used
            if (frameParameters.FragmentationProfile != null && frameParameters.FragmentationProfile.Length > 0)
            {
                // Convert the fragmentation profile into an array of bytes
                var byteArray = ConvertToBlob(frameParameters.FragmentationProfile);
                string base64String = Convert.ToBase64String(byteArray, 0, byteArray.Length);
                frameParams.Add(FrameParamKeyType.FragmentationProfile, base64String);
            }

            return frameParams;
        }

        /// <summary>
        /// Convert a frame parameter dictionary to an instance of the <see cref="FrameParams"/> class
        /// </summary>
        /// <param name="frameParamsByType"></param>
        /// <returns></returns>
        public static FrameParams ConvertStringParamsToFrameParams(Dictionary<FrameParamKeyType, string> frameParamsByType)
        {
            var frameParams = new FrameParams();

            foreach (var paramItem in frameParamsByType)
            {
                frameParams.AddUpdateValue(paramItem.Key, paramItem.Value);
            }

            return frameParams;
        }
    
#pragma warning disable 612, 618
        /// <summary>
        /// Obtain a FrameParameters instance from a FrameParams instance
        /// </summary>
        /// <param name="frameParams"><see cref="FrameParams"/> instance</param>
        /// <returns>A new <see cref="FrameParameters"/> instance</returns>
        public static FrameParameters GetLegacyFrameParameters(FrameParams frameParams)
        {
            if (frameParams == null)
                return new FrameParameters();

            var frametype = frameParams.FrameType;

            // Populate legacyFrameParams using dictionary frameParams
            var legacyFrameParams = new FrameParameters
            {
                StartTime = frameParams.GetValueDouble(FrameParamKeyType.StartTimeMinutes, 0),
                Duration = frameParams.GetValueDouble(FrameParamKeyType.DurationSeconds, 0),
                Accumulations = frameParams.GetValueInt32(FrameParamKeyType.Accumulations, 0),
                FrameType = (DataReader.FrameType)frametype,
                Decoded = frameParams.GetValueInt32(FrameParamKeyType.Decoded, 0),
                CalibrationDone = frameParams.GetValueInt32(FrameParamKeyType.CalibrationDone, 0),
                Scans = frameParams.Scans,
                IMFProfile = frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence, String.Empty),
                MPBitOrder = (short)frameParams.GetValueInt32(FrameParamKeyType.MPBitOrder, 0),
                TOFLosses = frameParams.GetValueDouble(FrameParamKeyType.TOFLosses, 0),
                AverageTOFLength = frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0),
                CalibrationSlope = frameParams.GetValueDouble(FrameParamKeyType.CalibrationSlope, 0),
                CalibrationIntercept = frameParams.GetValueDouble(FrameParamKeyType.CalibrationIntercept, 0),
                a2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficienta2, 0),
                b2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientb2, 0),
                c2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientc2, 0),
                d2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientd2, 0),
                e2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficiente2, 0),
                f2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientf2, 0),
                Temperature = frameParams.GetValueDouble(FrameParamKeyType.AmbientTemperature, 0),
                voltHVRack1 = frameParams.GetValueDouble(FrameParamKeyType.VoltHVRack1, 0),
                voltHVRack2 = frameParams.GetValueDouble(FrameParamKeyType.VoltHVRack2, 0),
                voltHVRack3 = frameParams.GetValueDouble(FrameParamKeyType.VoltHVRack3, 0),
                voltHVRack4 = frameParams.GetValueDouble(FrameParamKeyType.VoltHVRack4, 0),
                voltCapInlet = frameParams.GetValueDouble(FrameParamKeyType.VoltCapInlet, 0),
                voltEntranceHPFIn = frameParams.GetValueDouble(FrameParamKeyType.VoltEntranceHPFIn, 0),
                voltEntranceHPFOut = frameParams.GetValueDouble(FrameParamKeyType.VoltEntranceHPFOut, 0),
                voltEntranceCondLmt = frameParams.GetValueDouble(FrameParamKeyType.VoltEntranceCondLmt, 0),
                voltTrapOut = frameParams.GetValueDouble(FrameParamKeyType.VoltTrapOut, 0),
                voltTrapIn = frameParams.GetValueDouble(FrameParamKeyType.VoltTrapIn, 0),
                voltJetDist = frameParams.GetValueDouble(FrameParamKeyType.VoltJetDist, 0),
                voltQuad1 = frameParams.GetValueDouble(FrameParamKeyType.VoltQuad1, 0),
                voltCond1 = frameParams.GetValueDouble(FrameParamKeyType.VoltCond1, 0),
                voltQuad2 = frameParams.GetValueDouble(FrameParamKeyType.VoltQuad2, 0),
                voltCond2 = frameParams.GetValueDouble(FrameParamKeyType.VoltCond2, 0),
                voltIMSOut = frameParams.GetValueDouble(FrameParamKeyType.VoltIMSOut, 0),
                voltExitHPFIn = frameParams.GetValueDouble(FrameParamKeyType.VoltExitHPFIn, 0),
                voltExitHPFOut = frameParams.GetValueDouble(FrameParamKeyType.VoltExitHPFOut, 0),
                voltExitCondLmt = frameParams.GetValueDouble(FrameParamKeyType.VoltExitCondLmt, 0),
                PressureFront = frameParams.GetValueDouble(FrameParamKeyType.PressureFront, 0),
                PressureBack = frameParams.GetValueDouble(FrameParamKeyType.PressureBack, 0),
                HighPressureFunnelPressure = frameParams.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure, 0),
                IonFunnelTrapPressure = frameParams.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure, 0),
                RearIonFunnelPressure = frameParams.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure, 0),
                QuadrupolePressure = frameParams.GetValueDouble(FrameParamKeyType.QuadrupolePressure, 0),
                ESIVoltage = frameParams.GetValueDouble(FrameParamKeyType.ESIVoltage, 0),
                FloatVoltage = frameParams.GetValueDouble(FrameParamKeyType.FloatVoltage, 0)
            };

            var fragmentationProfile = frameParams.GetValue(FrameParamKeyType.FragmentationProfile, String.Empty);

            // ToDo: xxx implement this conversion xxx
            //legacyFrameParams.FragmentationProfile = Byte

            return legacyFrameParams;
        }
#pragma warning restore 612, 618

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

            // On the first iteration of the while loop, we require a case-sensitive match
            // If no match is found, then on the second iteration we use a case-insensitive match
            while (iteration < 2)
            {

                try
                {
                    // Note that this conversion works for both the names in the FrameParamKeyType enum and for the integer values
                    // See MSDN's "Enum.Parse Method" page at http://msdn.microsoft.com/en-us/library/essfb559.aspx
                    bool ignoreCase = iteration > 0;
                    var paramType = (FrameParamKeyType)Enum.Parse(typeof(FrameParamKeyType), paramName, ignoreCase);
                    if (Enum.IsDefined(typeof(FrameParamKeyType), paramType) | paramType.ToString().Contains(","))
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
        /// <returns><see cref="FrameParamDef"/> instance</returns>
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
        /// <returns><see cref="FrameParamDef"/> instance</returns>
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
                    throw new ArgumentOutOfRangeException("paramType", "Unrecognized frame param enum for paramType: " + (int)paramType);
            }

        }        

        #region "Private methods"

        /// <summary>
        /// Convert an array of doubles to an array of bytes
        /// </summary>
        /// <param name="frag">
        /// </param>
        /// <returns>
        /// Byte array
        /// </returns>
        private static byte[] ConvertToBlob(double[] frag)
        {
            
            int length_blob = frag.Length;
            var blob_values = new byte[length_blob * 8];

            Buffer.BlockCopy(frag, 0, blob_values, 0, length_blob * 8);

            return blob_values;
        }

        #endregion
   
    }

}
