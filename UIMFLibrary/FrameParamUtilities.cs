using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    /// <summary>
    /// Utility functions for working with frame parameters
    /// </summary>
    public static class FrameParamUtilities
    {

        /// <summary>
        /// Convert the array of bytes defining a fragmentation sequence to an array of doubles
        /// </summary>
        /// <param name="blob">
        /// </param>
        /// <returns>
        /// Array of doubles
        /// </returns>
        public static double[] ConvertByteArrayToFragmentationSequence(byte[] blob)
        {
            var frag = new double[blob.Length / 8];

            for (var i = 0; i < frag.Length; i++)
            {
                frag[i] = BitConverter.ToDouble(blob, i * 8);
            }

            return frag;
        }

        /// <summary>
        /// Convert an array of doubles to an array of bytes
        /// </summary>
        /// <param name="frag">
        /// </param>
        /// <returns>
        /// Byte array
        /// </returns>
        public static byte[] ConvertToBlob(double[] frag)
        {
            if (frag == null)
                frag = new double[0];

            // convert the fragmentation profile into an array of bytes
            var length_blob = frag.Length;
            var blob_values = new byte[length_blob * 8];

            Buffer.BlockCopy(frag, 0, blob_values, 0, length_blob * 8);

            return blob_values;
        }

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
	            {FrameParamKeyType.StartTimeMinutes, UIMFDataUtilities.DoubleToString(frameParameters.StartTime)},
                
	            // Duration of frame, in seconds
	            {FrameParamKeyType.DurationSeconds, UIMFDataUtilities.DoubleToString(frameParameters.Duration)},
                
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
                {FrameParamKeyType.AverageTOFLength, UIMFDataUtilities.DoubleToString(frameParameters.AverageTOFLength)},

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
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficienta2, UIMFDataUtilities.DoubleToString(frameParameters.a2));
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientb2, UIMFDataUtilities.DoubleToString(frameParameters.b2));
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientc2, UIMFDataUtilities.DoubleToString(frameParameters.c2));
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientd2, UIMFDataUtilities.DoubleToString(frameParameters.d2));
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficiente2, UIMFDataUtilities.DoubleToString(frameParameters.e2));
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientf2, UIMFDataUtilities.DoubleToString(frameParameters.f2));
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
                // Convert the fragmentation profile (array of doubles) into an array of bytes
                var byteArray = ConvertToBlob(frameParameters.FragmentationProfile);

                // Now convert to a base-64 encoded string
                var base64String = Convert.ToBase64String(byteArray, 0, byteArray.Length);

                // Finally, store in frameParams
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
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="frameParameters"><see cref="FrameParams"/> instance</param>
        /// <returns>A new <see cref="FrameParameters"/> instance</returns>
        public static FrameParameters GetLegacyFrameParameters(int frameNumber, FrameParams frameParameters)
        {
            if (frameParameters == null)
                return new FrameParameters();

            var frametype = frameParameters.FrameType;

            // Populate legacyFrameParams using dictionary frameParams
            var legacyFrameParams = new FrameParameters
            {
                FrameNum = frameNumber,
                StartTime = frameParameters.GetValueDouble(FrameParamKeyType.StartTimeMinutes, 0),
                Duration = frameParameters.GetValueDouble(FrameParamKeyType.DurationSeconds, 0),
                Accumulations = frameParameters.GetValueInt32(FrameParamKeyType.Accumulations, 0),
                FrameType = frametype,
                Decoded = frameParameters.GetValueInt32(FrameParamKeyType.Decoded, 0),
                CalibrationDone = frameParameters.GetValueInt32(FrameParamKeyType.CalibrationDone, 0),
                Scans = frameParameters.Scans,
                IMFProfile = frameParameters.GetValue(FrameParamKeyType.MultiplexingEncodingSequence, String.Empty),
                MPBitOrder = (short)frameParameters.GetValueInt32(FrameParamKeyType.MPBitOrder, 0),
                TOFLosses = frameParameters.GetValueDouble(FrameParamKeyType.TOFLosses, 0),
                AverageTOFLength = frameParameters.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0),
                CalibrationSlope = frameParameters.GetValueDouble(FrameParamKeyType.CalibrationSlope, 0),
                CalibrationIntercept = frameParameters.GetValueDouble(FrameParamKeyType.CalibrationIntercept, 0),
                a2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficienta2, 0),
                b2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientb2, 0),
                c2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientc2, 0),
                d2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientd2, 0),
                e2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficiente2, 0),
                f2 = frameParameters.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientf2, 0),
                Temperature = frameParameters.GetValueDouble(FrameParamKeyType.AmbientTemperature, 0),
                voltHVRack1 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack1, 0),
                voltHVRack2 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack2, 0),
                voltHVRack3 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack3, 0),
                voltHVRack4 = frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack4, 0),
                voltCapInlet = frameParameters.GetValueDouble(FrameParamKeyType.VoltCapInlet, 0),
                voltEntranceHPFIn = frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceHPFIn, 0),
                voltEntranceHPFOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceHPFOut, 0),
                voltEntranceCondLmt = frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceCondLmt, 0),
                voltTrapOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltTrapOut, 0),
                voltTrapIn = frameParameters.GetValueDouble(FrameParamKeyType.VoltTrapIn, 0),
                voltJetDist = frameParameters.GetValueDouble(FrameParamKeyType.VoltJetDist, 0),
                voltQuad1 = frameParameters.GetValueDouble(FrameParamKeyType.VoltQuad1, 0),
                voltCond1 = frameParameters.GetValueDouble(FrameParamKeyType.VoltCond1, 0),
                voltQuad2 = frameParameters.GetValueDouble(FrameParamKeyType.VoltQuad2, 0),
                voltCond2 = frameParameters.GetValueDouble(FrameParamKeyType.VoltCond2, 0),
                voltIMSOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltIMSOut, 0),
                voltExitHPFIn = frameParameters.GetValueDouble(FrameParamKeyType.VoltExitHPFIn, 0),
                voltExitHPFOut = frameParameters.GetValueDouble(FrameParamKeyType.VoltExitHPFOut, 0),
                voltExitCondLmt = frameParameters.GetValueDouble(FrameParamKeyType.VoltExitCondLmt, 0),
                PressureFront = frameParameters.GetValueDouble(FrameParamKeyType.PressureFront, 0),
                PressureBack = frameParameters.GetValueDouble(FrameParamKeyType.PressureBack, 0),
                HighPressureFunnelPressure = frameParameters.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure, 0),
                IonFunnelTrapPressure = frameParameters.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure, 0),
                RearIonFunnelPressure = frameParameters.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure, 0),
                QuadrupolePressure = frameParameters.GetValueDouble(FrameParamKeyType.QuadrupolePressure, 0),
                ESIVoltage = frameParameters.GetValueDouble(FrameParamKeyType.ESIVoltage, 0),
                FloatVoltage = frameParameters.GetValueDouble(FrameParamKeyType.FloatVoltage, 0)
            };

            var fragmentationProfile = frameParameters.GetValue(FrameParamKeyType.FragmentationProfile, String.Empty);

            if (string.IsNullOrEmpty(fragmentationProfile))
            {
                legacyFrameParams.FragmentationProfile = new double[0];
            }
            else
            {
                // The fragmentation profile was stored as an array of bytes, encoded as base 64
                
                // Convert back to bytes
                var byteArray = Convert.FromBase64String(fragmentationProfile);
                
                // Now convert from array of bytes to array of doubles
                legacyFrameParams.FragmentationProfile = ConvertByteArrayToFragmentationSequence(byteArray);
            }

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
            var iteration = 0;

            // On the first iteration of the while loop, we require a case-sensitive match
            // If no match is found, then on the second iteration we use a case-insensitive match
            while (iteration < 2)
            {

                try
                {
                    // Note that this conversion works for both the names in the FrameParamKeyType enum and for the integer values
                    // See MSDN's "Enum.Parse Method" page at http://msdn.microsoft.com/en-us/library/essfb559.aspx
                    var ignoreCase = iteration > 0;
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
                    return FrameParamKeyType.MassCalibrationCoefficienta2;
                case "b2":
                    return FrameParamKeyType.MassCalibrationCoefficientb2;
                case "c2":
                    return FrameParamKeyType.MassCalibrationCoefficientc2;
                case "d2":
                    return FrameParamKeyType.MassCalibrationCoefficientd2;
                case "e2":
                    return FrameParamKeyType.MassCalibrationCoefficiente2;
                case "f2":
                    return FrameParamKeyType.MassCalibrationCoefficientf2;

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
                throw new ArgumentOutOfRangeException(nameof(paramName), "paramName is empty");

            var paramType = GetParamTypeByName(paramName);

            if (paramType == FrameParamKeyType.Unknown)
                throw new ArgumentOutOfRangeException(nameof(paramName), "unknown value for paramName: " + paramName);

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
                    return new FrameParamDef(FrameParamKeyType.StartTimeMinutes, "StartTime", typeof(double),
                                          "Start time of frame, in minutes");

                case FrameParamKeyType.DurationSeconds:
                    return new FrameParamDef(FrameParamKeyType.DurationSeconds,
                                          FrameParamKeyType.DurationSeconds.ToString(), typeof(double),
                                          "Frame duration, in seconds");

                case FrameParamKeyType.Accumulations:
                    return new FrameParamDef(FrameParamKeyType.Accumulations, FrameParamKeyType.Accumulations.ToString(),
                                          typeof(int),
                                          "Number of collected and summed acquisitions in a frame");

                case FrameParamKeyType.FrameType:
                    // Allowed values defined by DataReader.FrameType
                    return new FrameParamDef(FrameParamKeyType.FrameType, FrameParamKeyType.FrameType.ToString(), typeof(int),
                                          "Frame Type: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan");

                case FrameParamKeyType.Decoded:
                    // Allowed values are 0 or 1
                    return new FrameParamDef(FrameParamKeyType.Decoded, FrameParamKeyType.Decoded.ToString(), typeof(int),
                                          "Tracks whether frame has been decoded: 0 for non-multiplexed or encoded; 1 if decoded");

                case FrameParamKeyType.CalibrationDone:
                    // Allowed values are 0 or 1, though -1 was used in the past instead of 0
                    return new FrameParamDef(FrameParamKeyType.CalibrationDone,
                                          FrameParamKeyType.CalibrationDone.ToString(), typeof(int),
                                          "Tracks whether frame has been calibrated: 1 if calibrated");

                case FrameParamKeyType.Scans:
                    return new FrameParamDef(FrameParamKeyType.Scans, FrameParamKeyType.Scans.ToString(), typeof(int),
                                          "Number of TOF scans in a frame");

                case FrameParamKeyType.MultiplexingEncodingSequence:
                    return new FrameParamDef(FrameParamKeyType.MultiplexingEncodingSequence,
                                          FrameParamKeyType.MultiplexingEncodingSequence.ToString(), typeof(string),
                                          "The name of the sequence used to encode the data when acquiring multiplexed data");

                case FrameParamKeyType.MPBitOrder:
                    return new FrameParamDef(FrameParamKeyType.MPBitOrder, FrameParamKeyType.MPBitOrder.ToString(), typeof(int),
                                          "Multiplexing bit order; Determines size of the bit sequence");

                case FrameParamKeyType.TOFLosses:
                    return new FrameParamDef(FrameParamKeyType.TOFLosses, FrameParamKeyType.TOFLosses.ToString(), typeof(int),
                                          "Number of TOF Losses (lost/skipped scans due to I/O problems)");

                case FrameParamKeyType.AverageTOFLength:
                    return new FrameParamDef(FrameParamKeyType.AverageTOFLength,
                                          FrameParamKeyType.AverageTOFLength.ToString(), typeof(double),
                                          "Average time between TOF trigger pulses, in nanoseconds");

                case FrameParamKeyType.CalibrationSlope:
                    return new FrameParamDef(FrameParamKeyType.CalibrationSlope,
                                          FrameParamKeyType.CalibrationSlope.ToString(), typeof(double),
                                          "Calibration slope, k0");

                case FrameParamKeyType.CalibrationIntercept:
                    return new FrameParamDef(FrameParamKeyType.CalibrationIntercept,
                                          FrameParamKeyType.CalibrationIntercept.ToString(), typeof(double),
                                          "Calibration intercept, t0");

                case FrameParamKeyType.MassCalibrationCoefficienta2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficienta2,
                                          FrameParamKeyType.MassCalibrationCoefficienta2.ToString(), typeof(double),
                                          "a2 parameter for residual mass error correction; ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11");

                case FrameParamKeyType.MassCalibrationCoefficientb2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientb2,
                                          FrameParamKeyType.MassCalibrationCoefficientb2.ToString(), typeof(double),
                                          "b2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientc2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientc2,
                                          FrameParamKeyType.MassCalibrationCoefficientc2.ToString(), typeof(double),
                                          "c2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientd2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientd2,
                                          FrameParamKeyType.MassCalibrationCoefficientd2.ToString(), typeof(double),
                                          "db2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficiente2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficiente2,
                                          FrameParamKeyType.MassCalibrationCoefficiente2.ToString(), typeof(double),
                                          "e2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientf2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientf2,
                                          FrameParamKeyType.MassCalibrationCoefficientf2.ToString(), typeof(double),
                                          "f2 parameter for residual mass error correction");

                case FrameParamKeyType.AmbientTemperature:
                    return new FrameParamDef(FrameParamKeyType.AmbientTemperature,
                                          FrameParamKeyType.AmbientTemperature.ToString(), typeof(float),
                                          "Ambient temperature, in Celcius");

                case FrameParamKeyType.VoltHVRack1:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack1, FrameParamKeyType.VoltHVRack1.ToString(),
                                          typeof(float),
                                          "Volt hv rack 1");

                case FrameParamKeyType.VoltHVRack2:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack2, FrameParamKeyType.VoltHVRack2.ToString(),
                                          typeof(float),
                                          "Volt hv rack 2");

                case FrameParamKeyType.VoltHVRack3:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack3, FrameParamKeyType.VoltHVRack3.ToString(),
                                          typeof(float),
                                          "Volt hv rack 3");

                case FrameParamKeyType.VoltHVRack4:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack4, FrameParamKeyType.VoltHVRack4.ToString(),
                                          typeof(float),
                                          "Volt hv rack 4");

                case FrameParamKeyType.VoltCapInlet:
                    return new FrameParamDef(FrameParamKeyType.VoltCapInlet, FrameParamKeyType.VoltCapInlet.ToString(),
                                          typeof(float),
                                          "Capillary Inlet Voltage");

                case FrameParamKeyType.VoltEntranceHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFIn,
                                          FrameParamKeyType.VoltEntranceHPFIn.ToString(), typeof(float),
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltEntranceHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFOut,
                                          FrameParamKeyType.VoltEntranceHPFOut.ToString(), typeof(float),
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltEntranceCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceCondLmt,
                                          FrameParamKeyType.VoltEntranceCondLmt.ToString(), typeof(float),
                                          "Entrance Cond Limit Voltage");

                case FrameParamKeyType.VoltTrapOut:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapOut, FrameParamKeyType.VoltTrapOut.ToString(),
                                          typeof(float),
                                          "Trap Out Voltage");

                case FrameParamKeyType.VoltTrapIn:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapIn, FrameParamKeyType.VoltTrapIn.ToString(), typeof(float),
                                          "Trap In Voltage");

                case FrameParamKeyType.VoltJetDist:
                    return new FrameParamDef(FrameParamKeyType.VoltJetDist, FrameParamKeyType.VoltJetDist.ToString(),
                                          typeof(float),
                                          "Jet Disruptor Voltage");

                case FrameParamKeyType.VoltQuad1:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad1, FrameParamKeyType.VoltQuad1.ToString(), typeof(float),
                                          "Fragmentation Quadrupole Voltage 1");

                case FrameParamKeyType.VoltCond1:
                    return new FrameParamDef(FrameParamKeyType.VoltCond1, FrameParamKeyType.VoltCond1.ToString(), typeof(float),
                                          "Fragmentation Conductance Voltage 1");

                case FrameParamKeyType.VoltQuad2:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad2, FrameParamKeyType.VoltQuad2.ToString(), typeof(float),
                                          "Fragmentation Quadrupole Voltage 2");

                case FrameParamKeyType.VoltCond2:
                    return new FrameParamDef(FrameParamKeyType.VoltCond2, FrameParamKeyType.VoltCond2.ToString(), typeof(float),
                                          "Fragmentation Conductance Voltage 2");

                case FrameParamKeyType.VoltIMSOut:
                    return new FrameParamDef(FrameParamKeyType.VoltIMSOut, FrameParamKeyType.VoltIMSOut.ToString(), typeof(float),
                                          "IMS Out Voltage");

                case FrameParamKeyType.VoltExitHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFIn, FrameParamKeyType.VoltExitHPFIn.ToString(),
                                          typeof(float),
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltExitHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFOut, FrameParamKeyType.VoltExitHPFOut.ToString(),
                                          typeof(float),
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltExitCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltExitCondLmt,
                                          FrameParamKeyType.VoltExitCondLmt.ToString(), typeof(float),
                                          "Exit Cond Limit Voltage");

                case FrameParamKeyType.PressureFront:
                    return new FrameParamDef(FrameParamKeyType.PressureFront, FrameParamKeyType.PressureFront.ToString(),
                                          typeof(float),
                                          "Pressure at front of Drift Tube");

                case FrameParamKeyType.PressureBack:
                    return new FrameParamDef(FrameParamKeyType.PressureBack, FrameParamKeyType.PressureBack.ToString(),
                                          typeof(float),
                                          "Pressure at back of Drift Tube");

                case FrameParamKeyType.HighPressureFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.HighPressureFunnelPressure,
                                          FrameParamKeyType.HighPressureFunnelPressure.ToString(), typeof(float),
                                          "High pressure funnel pressure");

                case FrameParamKeyType.IonFunnelTrapPressure:
                    return new FrameParamDef(FrameParamKeyType.IonFunnelTrapPressure,
                                          FrameParamKeyType.IonFunnelTrapPressure.ToString(), typeof(float),
                                          "Ion funnel trap pressure");

                case FrameParamKeyType.RearIonFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.RearIonFunnelPressure,
                                          FrameParamKeyType.RearIonFunnelPressure.ToString(), typeof(float),
                                          "Rear ion funnel pressure");

                case FrameParamKeyType.QuadrupolePressure:
                    return new FrameParamDef(FrameParamKeyType.QuadrupolePressure,
                                          FrameParamKeyType.QuadrupolePressure.ToString(), typeof(float),
                                          "Quadrupole pressure");

                case FrameParamKeyType.ESIVoltage:
                    return new FrameParamDef(FrameParamKeyType.ESIVoltage, FrameParamKeyType.ESIVoltage.ToString(), typeof(float),
                                          "ESI voltage");

                case FrameParamKeyType.FloatVoltage:
                    return new FrameParamDef(FrameParamKeyType.FloatVoltage, FrameParamKeyType.FloatVoltage.ToString(),
                                          typeof(float),
                                          "Float voltage");

                case FrameParamKeyType.FragmentationProfile:
                    return new FrameParamDef(FrameParamKeyType.FragmentationProfile,
                                          FrameParamKeyType.FragmentationProfile.ToString(), typeof(string),
                                          "Voltage profile used in fragmentation (array of doubles, converted to an array of bytes, then stored as a Base 64 encoded string)");

                case FrameParamKeyType.ScanNumFirst:
                    return new FrameParamDef(FrameParamKeyType.ScanNumFirst, FrameParamKeyType.ScanNumFirst.ToString(),
                                          typeof(int),
                                          "First scan");

                case FrameParamKeyType.ScanNumLast:
                    return new FrameParamDef(FrameParamKeyType.ScanNumLast, FrameParamKeyType.ScanNumLast.ToString(),
                                          typeof(int),
                                          "Last scan");

                case FrameParamKeyType.PressureUnits:
                    return new FrameParamDef(FrameParamKeyType.PressureUnits, FrameParamKeyType.PressureUnits.ToString(),
                                          typeof(PressureUnits),
                                          "Units for pressure");


                default:
                    throw new ArgumentOutOfRangeException(nameof(paramType), "Unrecognized frame param enum for paramType: " + (int)paramType);
            }

        }        

    }

}
