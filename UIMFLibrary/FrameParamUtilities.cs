using System;
using System.Collections.Generic;

namespace UIMFLibrary
{
    /// <summary>
    /// Utility functions for working with frame parameters
    /// </summary>
    public static class FrameParamUtilities
    {
        #region Member variables

        private static readonly Dictionary<Type, dynamic> mDefaultValuesByType = new Dictionary<Type, dynamic>();

        private static readonly Dictionary<FrameParamKeyType, Type> mFrameParamKeyTypes = new Dictionary<FrameParamKeyType, Type>();

        #endregion

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
        /// Convert a dynamic value to a string
        /// </summary>
        /// <param name="value"></param>
        /// <param name="convertedValue"></param>
        /// <returns></returns>
        public static bool ConvertDynamicToDouble(dynamic value, out double convertedValue)
        {
            if (value is double || value is float || value is int || value is short || value is byte)
            {
                convertedValue = value;
                return true;
            }

            if (value is string)
            {
                if (double.TryParse(value, out double result))
                {
                    convertedValue = result;
                    return true;
                }
            }
            else
            {
                if (double.TryParse(value.ToString(), out double result))
                {
                    convertedValue = result;
                    return true;
                }
            }

            convertedValue = 0;
            return false;
        }

        /// <summary>
        /// Convert a dynamic value to an integer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="convertedValue"></param>
        /// <returns></returns>
        public static bool ConvertDynamicToInt32(dynamic value, out int convertedValue)
        {
            if (value is int || value is short || value is byte)
            {
                convertedValue = value;
                return true;
            }

            if (value is string)
            {
                if (int.TryParse(value, out int result))
                {
                    convertedValue = result;
                    return true;
                }
            }
            else
            {
                if (int.TryParse(value.ToString(), out int result))
                {
                    convertedValue = result;
                    return true;
                }
            }

            convertedValue = 0;
            return false;
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
        public static Dictionary<FrameParamKeyType, dynamic> ConvertFrameParameters(FrameParameters frameParameters)
#pragma warning restore 612, 618
        {
            var frameParams = new Dictionary<FrameParamKeyType, dynamic>
            {
                // Start time of frame, in minutes
                {FrameParamKeyType.StartTimeMinutes, frameParameters.StartTime},

                // Duration of frame, in seconds
                {FrameParamKeyType.DurationSeconds, frameParameters.Duration},

                // Number of collected and summed acquisitions in a frame
                {FrameParamKeyType.Accumulations, frameParameters.Accumulations},

                // Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
                {FrameParamKeyType.FrameType, (int)frameParameters.FrameType},

                // Set to 1 after a frame has been decoded (added June 27, 2011)
                {FrameParamKeyType.Decoded, frameParameters.Decoded},

                // Set to 1 after a frame has been calibrated
                {FrameParamKeyType.CalibrationDone, frameParameters.CalibrationDone},

                // Number of TOF scans
                {FrameParamKeyType.Scans, frameParameters.Scans},

                // IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed
                {FrameParamKeyType.MultiplexingEncodingSequence, frameParameters.IMFProfile},

                // Original size of bit sequence
                {FrameParamKeyType.MPBitOrder, frameParameters.MPBitOrder},

                // Number of TOF Losses
                {FrameParamKeyType.TOFLosses, frameParameters.TOFLosses},

                // Average time between TOF trigger pulses
                {FrameParamKeyType.AverageTOFLength, frameParameters.AverageTOFLength},

                // Calibration slope, k0
                {FrameParamKeyType.CalibrationSlope, frameParameters.CalibrationSlope},

                // Calibration intercept, t0
                {FrameParamKeyType.CalibrationIntercept, frameParameters.CalibrationIntercept}
            };

            // These six parameters are coefficients for residual mass error correction
            // ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11
            if (Math.Abs(frameParameters.a2) > float.Epsilon ||
                Math.Abs(frameParameters.b2) > float.Epsilon ||
                Math.Abs(frameParameters.c2) > float.Epsilon ||
                Math.Abs(frameParameters.d2) > float.Epsilon ||
                Math.Abs(frameParameters.e2) > float.Epsilon ||
                Math.Abs(frameParameters.f2) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficienta2, frameParameters.a2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientb2, frameParameters.b2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientc2, frameParameters.c2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientd2, frameParameters.d2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficiente2, frameParameters.e2);
                frameParams.Add(FrameParamKeyType.MassCalibrationCoefficientf2, frameParameters.f2);
            }

            // Ambient temperature
            frameParams.Add(FrameParamKeyType.AmbientTemperature, frameParameters.Temperature);

            // Voltage settings in the IMS system
            if (Math.Abs(frameParameters.voltHVRack1) > float.Epsilon ||
                Math.Abs(frameParameters.voltHVRack2) > float.Epsilon ||
                Math.Abs(frameParameters.voltHVRack3) > float.Epsilon ||
                Math.Abs(frameParameters.voltHVRack4) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltHVRack1, frameParameters.voltHVRack1);
                frameParams.Add(FrameParamKeyType.VoltHVRack2, frameParameters.voltHVRack2);
                frameParams.Add(FrameParamKeyType.VoltHVRack3, frameParameters.voltHVRack3);
                frameParams.Add(FrameParamKeyType.VoltHVRack4, frameParameters.voltHVRack4);
            }

            // Capillary Inlet Voltage
            // HPF In Voltage
            // HPF Out Voltage
            // Cond Limit Voltage
            if (Math.Abs(frameParameters.voltEntranceHPFIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltEntranceHPFIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltEntranceHPFOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltEntranceCondLmt) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltCapInlet, frameParameters.voltCapInlet);
                frameParams.Add(FrameParamKeyType.VoltEntranceHPFIn, frameParameters.voltEntranceHPFIn);
                frameParams.Add(FrameParamKeyType.VoltEntranceHPFOut, frameParameters.voltEntranceHPFOut);
                frameParams.Add(FrameParamKeyType.VoltEntranceCondLmt, frameParameters.voltEntranceCondLmt);
            }

            // Trap Out Voltage
            // Trap In Voltage
            // Jet Disruptor Voltage
            if (Math.Abs(frameParameters.voltTrapOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltTrapIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltJetDist) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltTrapOut, frameParameters.voltTrapOut);
                frameParams.Add(FrameParamKeyType.VoltTrapIn, frameParameters.voltTrapIn);
                frameParams.Add(FrameParamKeyType.VoltJetDist, frameParameters.voltJetDist);
            }

            // Fragmentation Quadrupole 1 Voltage
            // Fragmentation Conductance 1 Voltage
            if (Math.Abs(frameParameters.voltQuad1) > float.Epsilon ||
                Math.Abs(frameParameters.voltCond1) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltQuad1, frameParameters.voltQuad1);
                frameParams.Add(FrameParamKeyType.VoltCond1, frameParameters.voltCond1);
            }

            // Fragmentation Quadrupole 2 Voltage
            // Fragmentation Conductance 2 Voltage
            if (Math.Abs(frameParameters.voltQuad2) > float.Epsilon ||
                Math.Abs(frameParameters.voltCond2) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltQuad2, frameParameters.voltQuad2);
                frameParams.Add(FrameParamKeyType.VoltCond2, frameParameters.voltCond2);
            }

            // IMS Out Voltage
            // HPF In Voltage
            // HPF Out Voltage
            if (Math.Abs(frameParameters.voltIMSOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltExitHPFIn) > float.Epsilon ||
                Math.Abs(frameParameters.voltExitHPFOut) > float.Epsilon ||
                Math.Abs(frameParameters.voltExitCondLmt) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.VoltIMSOut, frameParameters.voltIMSOut);
                frameParams.Add(FrameParamKeyType.VoltExitHPFIn, frameParameters.voltExitHPFIn);
                frameParams.Add(FrameParamKeyType.VoltExitHPFOut, frameParameters.voltExitHPFOut);
                frameParams.Add(FrameParamKeyType.VoltExitCondLmt, frameParameters.voltExitCondLmt);
            }

            // Pressure at front of Drift Tube
            // Pressure at back of Drift Tube
            if (Math.Abs(frameParameters.PressureFront) > float.Epsilon ||
                Math.Abs(frameParameters.PressureBack) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.PressureFront, frameParameters.PressureFront);
                frameParams.Add(FrameParamKeyType.PressureBack, frameParameters.PressureBack);
            }

            // High pressure funnel pressure
            // Ion funnel trap pressure
            // Rear ion funnel pressure
            // Quadruple pressure
            if (Math.Abs(frameParameters.HighPressureFunnelPressure) > float.Epsilon ||
                Math.Abs(frameParameters.IonFunnelTrapPressure) > float.Epsilon ||
                Math.Abs(frameParameters.RearIonFunnelPressure) > float.Epsilon ||
                Math.Abs(frameParameters.QuadrupolePressure) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.HighPressureFunnelPressure, frameParameters.HighPressureFunnelPressure);
                frameParams.Add(FrameParamKeyType.IonFunnelTrapPressure, frameParameters.IonFunnelTrapPressure);
                frameParams.Add(FrameParamKeyType.RearIonFunnelPressure, frameParameters.RearIonFunnelPressure);
                frameParams.Add(FrameParamKeyType.QuadrupolePressure, frameParameters.QuadrupolePressure);
            }

            // ESI Voltage
            if (Math.Abs(frameParameters.ESIVoltage) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.ESIVoltage, frameParameters.ESIVoltage);
            }

            // Float Voltage
            if (Math.Abs(frameParameters.FloatVoltage) > float.Epsilon)
            {
                frameParams.Add(FrameParamKeyType.FloatVoltage, frameParameters.FloatVoltage);
            }

            // Voltage profile used in fragmentation
            // Legacy parameter, likely never used
            if (frameParameters.FragmentationProfile?.Length > 0)
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
        /// Convert the string value to a dynamic variable of the given type
        /// </summary>
        /// <param name="targetType"></param>
        /// <param name="value"></param>
        /// <param name="returnNullOnError">When true, return null if the conversion fails; when false, return the value as a string</param>
        /// <returns></returns>
        /// <remarks>
        /// Supports byte, short, int, float, double, and DateTime
        /// All other types will continue to be strings
        /// </remarks>
        public static dynamic ConvertStringToDynamic(Type targetType, string value, bool returnNullOnError = true)
        {
            try
            {
                if (value == null)
                    value = string.Empty;

                if (targetType == typeof(byte))
                {
                    if (value.EndsWith(".0"))
                        value = value.Substring(0, value.Length - 2);

                    if (byte.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(short))
                {
                    if (value.EndsWith(".0"))
                        value = value.Substring(0, value.Length - 2);

                    if (short.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(int))
                {
                    if (value.EndsWith(".0"))
                        value = value.Substring(0, value.Length - 2);

                    if (int.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(float))
                {
                    // Interpret null or empty string as NaN
                    if (string.IsNullOrEmpty(value))
                        return float.NaN;

                    if (value.Equals("Inf", StringComparison.OrdinalIgnoreCase))
                        return float.MaxValue;

                    if (value.Equals("-Inf", StringComparison.OrdinalIgnoreCase))
                        return float.MinValue;

                    if (float.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(double))
                {
                    // Interpret null or empty string as NaN
                    if (string.IsNullOrEmpty(value))
                        return double.NaN;

                    if (value.Equals("Inf", StringComparison.OrdinalIgnoreCase))
                        return double.MaxValue;

                    if (value.Equals("-Inf", StringComparison.OrdinalIgnoreCase))
                        return double.MinValue;

                    if (double.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(DateTime))
                {
                    if (DateTime.TryParse(value, out var parsed))
                        return parsed;

                    if (!returnNullOnError)
                        Console.WriteLine("Warning: cannot convert {0} to a {1}; will store as a string", value, targetType);
                }

                if (targetType == typeof(string))
                {
                    return value;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot convert {0} to a {1}: {2}", value, targetType, ex);
            }

            return returnNullOnError ? null : value;
        }

        /// <summary>
        /// Convert a frame parameter dictionary to an instance of the <see cref="FrameParams"/> class
        /// </summary>
        /// <param name="frameParamsByType"></param>
        /// <returns></returns>
        [Obsolete("Superseded by ConvertDynamicParamsToFrameParams")]
        // ReSharper disable once UnusedMember.Global
        public static FrameParams ConvertStringParamsToFrameParams(Dictionary<FrameParamKeyType, string> frameParamsByType)
        {
            var frameParams = new FrameParams();

            foreach (var paramItem in frameParamsByType)
            {
                frameParams.AddUpdateValue(paramItem.Key, paramItem.Value);
            }

            return frameParams;
        }

        /// <summary>
        /// Convert a frame parameter dictionary to an instance of the <see cref="FrameParams"/> class
        /// </summary>
        /// <param name="frameParamsByType"></param>
        /// <returns></returns>
        public static FrameParams ConvertDynamicParamsToFrameParams(Dictionary<FrameParamKeyType, dynamic> frameParamsByType)
        {
            var frameParams = new FrameParams();

            foreach (var paramItem in frameParamsByType)
            {
                frameParams.AddUpdateValue(paramItem.Key, paramItem.Value);
            }

            return frameParams;
        }

        /// <summary>
        /// Get the default value for the data type associated with teh given frame param key
        /// </summary>
        /// <param name="paramType"></param>
        /// <returns></returns>
        public static dynamic GetDefaultValueByType(FrameParamKeyType paramType)
        {
            var dataType = GetFrameParamKeyDataType(paramType);
            return GetDefaultValueByType(dataType);
        }

        /// <summary>
        /// Get the default value for the given data type
        /// </summary>
        /// <param name="dataType"></param>
        /// <returns></returns>
        /// <remarks>This method is used by this class and by GlobalParamUtilities</remarks>
        public static dynamic GetDefaultValueByType(Type dataType)
        {
            if (!dataType.IsValueType)
            {
                return dataType == typeof(string) ? string.Empty : null;
            }

            if (mDefaultValuesByType.TryGetValue(dataType, out var defaultValue))
                return defaultValue;

            var defaultForType = Activator.CreateInstance(dataType);
            mDefaultValuesByType.Add(dataType, defaultForType);

            return defaultForType;
        }

        /// <summary>
        /// Get the system data type associated with a given frame parameter key
        /// </summary>
        /// <param name="paramType"></param>
        /// <returns></returns>
        public static Type GetFrameParamKeyDataType(FrameParamKeyType paramType)
        {
            if (mFrameParamKeyTypes.Count == 0)
            {
                var keyTypes = new Dictionary<FrameParamKeyType, Type>
                {
                    {FrameParamKeyType.StartTimeMinutes, typeof(double)},
                    {FrameParamKeyType.DurationSeconds, typeof(double)},
                    {FrameParamKeyType.Accumulations, typeof(int)},
                    {FrameParamKeyType.FrameType, typeof(int)},
                    {FrameParamKeyType.Decoded, typeof(int)},
                    {FrameParamKeyType.CalibrationDone, typeof(int)},
                    {FrameParamKeyType.Scans, typeof(int)},
                    {FrameParamKeyType.MultiplexingEncodingSequence, typeof(string)},
                    {FrameParamKeyType.MPBitOrder, typeof(int)},
                    {FrameParamKeyType.TOFLosses, typeof(int)},
                    {FrameParamKeyType.AverageTOFLength, typeof(double)},
                    {FrameParamKeyType.CalibrationSlope, typeof(double)},
                    {FrameParamKeyType.CalibrationIntercept, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficienta2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientb2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientc2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientd2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficiente2, typeof(double)},
                    {FrameParamKeyType.MassCalibrationCoefficientf2, typeof(double)},
                    {FrameParamKeyType.AmbientTemperature, typeof(float)},
                    {FrameParamKeyType.DriftTubeTemperature, typeof(float)},
                    {FrameParamKeyType.VoltHVRack1, typeof(float)},
                    {FrameParamKeyType.VoltHVRack2, typeof(float)},
                    {FrameParamKeyType.VoltHVRack3, typeof(float)},
                    {FrameParamKeyType.VoltHVRack4, typeof(float)},
                    {FrameParamKeyType.VoltCapInlet, typeof(float)},
                    {FrameParamKeyType.VoltEntranceHPFIn, typeof(float)},
                    {FrameParamKeyType.VoltEntranceHPFOut, typeof(float)},
                    {FrameParamKeyType.VoltEntranceCondLmt, typeof(float)},
                    {FrameParamKeyType.VoltTrapOut, typeof(float)},
                    {FrameParamKeyType.VoltTrapIn, typeof(float)},
                    {FrameParamKeyType.VoltJetDist, typeof(float)},
                    {FrameParamKeyType.VoltQuad1, typeof(float)},
                    {FrameParamKeyType.VoltCond1, typeof(float)},
                    {FrameParamKeyType.VoltQuad2, typeof(float)},
                    {FrameParamKeyType.VoltCond2, typeof(float)},
                    {FrameParamKeyType.VoltIMSOut, typeof(float)},
                    {FrameParamKeyType.VoltExitHPFIn, typeof(float)},
                    {FrameParamKeyType.VoltExitHPFOut, typeof(float)},
                    {FrameParamKeyType.VoltExitCondLmt, typeof(float)},
                    {FrameParamKeyType.PressureFront, typeof(float)},
                    {FrameParamKeyType.PressureBack, typeof(float)},
                    {FrameParamKeyType.HighPressureFunnelPressure, typeof(float)},
                    {FrameParamKeyType.IonFunnelTrapPressure, typeof(float)},
                    {FrameParamKeyType.RearIonFunnelPressure, typeof(float)},
                    {FrameParamKeyType.QuadrupolePressure, typeof(float)},
                    {FrameParamKeyType.ESIVoltage, typeof(float)},
                    {FrameParamKeyType.FloatVoltage, typeof(float)},
                    {FrameParamKeyType.FragmentationProfile, typeof(string)},
                    {FrameParamKeyType.ScanNumFirst, typeof(int)},
                    {FrameParamKeyType.ScanNumLast, typeof(int)},
                    {FrameParamKeyType.PressureUnits, typeof(PressureUnits)}
                };

                foreach (var item in keyTypes)
                    mFrameParamKeyTypes.Add(item.Key, item.Value);
            }

            if (mFrameParamKeyTypes.TryGetValue(paramType, out var dataType))
                return dataType;

            throw new ArgumentOutOfRangeException(nameof(paramType), "Unrecognized frame param enum for paramType: " + (int)paramType);
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

            var frameType = frameParameters.FrameType;

            // Populate legacyFrameParams using dictionary frameParams
            var legacyFrameParams = new FrameParameters
            {
                FrameNum = frameNumber,
                StartTime = frameParameters.GetValueDouble(FrameParamKeyType.StartTimeMinutes, 0),
                Duration = frameParameters.GetValueDouble(FrameParamKeyType.DurationSeconds, 0),
                Accumulations = frameParameters.GetValueInt32(FrameParamKeyType.Accumulations, 0),
                FrameType = frameType,
                Decoded = frameParameters.GetValueInt32(FrameParamKeyType.Decoded, 0),
                CalibrationDone = frameParameters.GetValueInt32(FrameParamKeyType.CalibrationDone, 0),
                Scans = frameParameters.Scans,
                IMFProfile = frameParameters.GetValue(FrameParamKeyType.MultiplexingEncodingSequence, string.Empty),
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

            var fragmentationProfile = frameParameters.GetValue(FrameParamKeyType.FragmentationProfile, string.Empty);

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
            // On the first iteration of the while loop, we require a case-sensitive match
            // If no match is found, then on the second iteration we use a case-insensitive match
            for (var iteration = 0; iteration < 2; iteration++)
            {
                try
                {
                    // Note that this conversion works for both the names in the FrameParamKeyType enum and for the integer values
                    // See MSDN's "Enum.Parse Method" page at http://msdn.microsoft.com/en-us/library/essfb559.aspx
                    var ignoreCase = iteration > 0;
                    var paramType = (FrameParamKeyType)Enum.Parse(typeof(FrameParamKeyType), paramName, ignoreCase);
                    if (Enum.IsDefined(typeof(FrameParamKeyType), paramType) || paramType.ToString().Contains(","))
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

                case "DriftTubeTemperature":
                    return FrameParamKeyType.DriftTubeTemperature;

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
        // ReSharper disable once UnusedMember.Global
        public static FrameParamDef GetParamDefByName(string paramName)
        {
            if (string.IsNullOrWhiteSpace(paramName))
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
            var targetType = GetFrameParamKeyDataType(paramType);

            switch (paramType)
            {
                case FrameParamKeyType.StartTimeMinutes:
                    return new FrameParamDef(FrameParamKeyType.StartTimeMinutes, "StartTime", targetType,
                                          "Start time of frame, in minutes");

                case FrameParamKeyType.DurationSeconds:
                    return new FrameParamDef(FrameParamKeyType.DurationSeconds,
                                          nameof(FrameParamKeyType.DurationSeconds), targetType,
                                          "Frame duration, in seconds");

                case FrameParamKeyType.Accumulations:
                    return new FrameParamDef(FrameParamKeyType.Accumulations, nameof(FrameParamKeyType.Accumulations),
                                          targetType,
                                          "Number of collected and summed acquisitions in a frame");

                case FrameParamKeyType.FrameType:
                    // Allowed values are defined by UIMFData.FrameType
                    return new FrameParamDef(FrameParamKeyType.FrameType, nameof(FrameParamKeyType.FrameType), targetType,
                                          "Frame Type: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan");

                case FrameParamKeyType.Decoded:
                    // Allowed values are 0 or 1
                    return new FrameParamDef(FrameParamKeyType.Decoded, nameof(FrameParamKeyType.Decoded), targetType,
                                          "Tracks whether frame has been decoded: 0 for non-multiplexed or encoded; 1 if decoded");

                case FrameParamKeyType.CalibrationDone:
                    // Allowed values are 0 or 1, though -1 was used in the past instead of 0
                    return new FrameParamDef(FrameParamKeyType.CalibrationDone,
                                          nameof(FrameParamKeyType.CalibrationDone), targetType,
                                          "Tracks whether frame has been calibrated: 1 if calibrated");

                case FrameParamKeyType.Scans:
                    return new FrameParamDef(FrameParamKeyType.Scans, nameof(FrameParamKeyType.Scans), targetType,
                                          "Number of TOF scans in a frame");

                case FrameParamKeyType.MultiplexingEncodingSequence:
                    return new FrameParamDef(FrameParamKeyType.MultiplexingEncodingSequence,
                                          nameof(FrameParamKeyType.MultiplexingEncodingSequence), targetType,
                                          "The name of the sequence used to encode the data when acquiring multiplexed data");

                case FrameParamKeyType.MPBitOrder:
                    return new FrameParamDef(FrameParamKeyType.MPBitOrder, nameof(FrameParamKeyType.MPBitOrder), targetType,
                                          "Multiplexing bit order; Determines size of the bit sequence");

                case FrameParamKeyType.TOFLosses:
                    return new FrameParamDef(FrameParamKeyType.TOFLosses, nameof(FrameParamKeyType.TOFLosses), targetType,
                                          "Number of TOF Losses (lost/skipped scans due to I/O problems)");

                case FrameParamKeyType.AverageTOFLength:
                    return new FrameParamDef(FrameParamKeyType.AverageTOFLength,
                                          nameof(FrameParamKeyType.AverageTOFLength), targetType,
                                          "Average time between TOF trigger pulses, in nanoseconds");

                case FrameParamKeyType.CalibrationSlope:
                    return new FrameParamDef(FrameParamKeyType.CalibrationSlope,
                                          nameof(FrameParamKeyType.CalibrationSlope), targetType,
                                          "Calibration slope; k is slope / 10000");

                case FrameParamKeyType.CalibrationIntercept:
                    return new FrameParamDef(FrameParamKeyType.CalibrationIntercept,
                                          nameof(FrameParamKeyType.CalibrationIntercept), targetType,
                                          "Calibration intercept; t0 is intercept * 10000");

                case FrameParamKeyType.MassCalibrationCoefficienta2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficienta2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficienta2), targetType,
                                          "a2 parameter for residual mass error correction; ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11");

                case FrameParamKeyType.MassCalibrationCoefficientb2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientb2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientb2), targetType,
                                          "b2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientc2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientc2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientc2), targetType,
                                          "c2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientd2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientd2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientd2), targetType,
                                          "db2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficiente2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficiente2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficiente2), targetType,
                                          "e2 parameter for residual mass error correction");

                case FrameParamKeyType.MassCalibrationCoefficientf2:
                    return new FrameParamDef(FrameParamKeyType.MassCalibrationCoefficientf2,
                                          nameof(FrameParamKeyType.MassCalibrationCoefficientf2), targetType,
                                          "f2 parameter for residual mass error correction");

                case FrameParamKeyType.AmbientTemperature:
                    return new FrameParamDef(FrameParamKeyType.AmbientTemperature,
                                          nameof(FrameParamKeyType.AmbientTemperature), targetType,
                                          "Ambient temperature, in Celsius");

                case FrameParamKeyType.DriftTubeTemperature:
                    return new FrameParamDef(FrameParamKeyType.DriftTubeTemperature,
                                             nameof(FrameParamKeyType.DriftTubeTemperature), targetType,
                                             "Drift tube temperature, in Celsius");

                case FrameParamKeyType.VoltHVRack1:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack1, nameof(FrameParamKeyType.VoltHVRack1),
                                          targetType,
                                          "Volt hv rack 1");

                case FrameParamKeyType.VoltHVRack2:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack2, nameof(FrameParamKeyType.VoltHVRack2),
                                          targetType,
                                          "Volt hv rack 2");

                case FrameParamKeyType.VoltHVRack3:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack3, nameof(FrameParamKeyType.VoltHVRack3),
                                          targetType,
                                          "Volt hv rack 3");

                case FrameParamKeyType.VoltHVRack4:
                    return new FrameParamDef(FrameParamKeyType.VoltHVRack4, nameof(FrameParamKeyType.VoltHVRack4),
                                          targetType,
                                          "Volt hv rack 4");

                case FrameParamKeyType.VoltCapInlet:
                    return new FrameParamDef(FrameParamKeyType.VoltCapInlet, nameof(FrameParamKeyType.VoltCapInlet),
                                          targetType,
                                          "Capillary Inlet Voltage");

                case FrameParamKeyType.VoltEntranceHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFIn,
                                          nameof(FrameParamKeyType.VoltEntranceHPFIn), targetType,
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltEntranceHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceHPFOut,
                                          nameof(FrameParamKeyType.VoltEntranceHPFOut), targetType,
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltEntranceCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltEntranceCondLmt,
                                          nameof(FrameParamKeyType.VoltEntranceCondLmt), targetType,
                                          "Entrance Cond Limit Voltage");

                case FrameParamKeyType.VoltTrapOut:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapOut, nameof(FrameParamKeyType.VoltTrapOut),
                                          targetType,
                                          "Trap Out Voltage");

                case FrameParamKeyType.VoltTrapIn:
                    return new FrameParamDef(FrameParamKeyType.VoltTrapIn, nameof(FrameParamKeyType.VoltTrapIn), targetType,
                                          "Trap In Voltage");

                case FrameParamKeyType.VoltJetDist:
                    return new FrameParamDef(FrameParamKeyType.VoltJetDist, nameof(FrameParamKeyType.VoltJetDist),
                                          targetType,
                                          "Jet Disruptor Voltage");

                case FrameParamKeyType.VoltQuad1:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad1, nameof(FrameParamKeyType.VoltQuad1), targetType,
                                          "Fragmentation Quadrupole Voltage 1");

                case FrameParamKeyType.VoltCond1:
                    return new FrameParamDef(FrameParamKeyType.VoltCond1, nameof(FrameParamKeyType.VoltCond1), targetType,
                                          "Fragmentation Conductance Voltage 1");

                case FrameParamKeyType.VoltQuad2:
                    return new FrameParamDef(FrameParamKeyType.VoltQuad2, nameof(FrameParamKeyType.VoltQuad2), targetType,
                                          "Fragmentation Quadrupole Voltage 2");

                case FrameParamKeyType.VoltCond2:
                    return new FrameParamDef(FrameParamKeyType.VoltCond2, nameof(FrameParamKeyType.VoltCond2), targetType,
                                          "Fragmentation Conductance Voltage 2");

                case FrameParamKeyType.VoltIMSOut:
                    return new FrameParamDef(FrameParamKeyType.VoltIMSOut, nameof(FrameParamKeyType.VoltIMSOut), targetType,
                                          "IMS Out Voltage");

                case FrameParamKeyType.VoltExitHPFIn:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFIn, nameof(FrameParamKeyType.VoltExitHPFIn),
                                          targetType,
                                          "HPF In Voltage");

                case FrameParamKeyType.VoltExitHPFOut:
                    return new FrameParamDef(FrameParamKeyType.VoltExitHPFOut, nameof(FrameParamKeyType.VoltExitHPFOut),
                                          targetType,
                                          "HPF Out Voltage");

                case FrameParamKeyType.VoltExitCondLmt:
                    return new FrameParamDef(FrameParamKeyType.VoltExitCondLmt,
                                          nameof(FrameParamKeyType.VoltExitCondLmt), targetType,
                                          "Exit Cond Limit Voltage");

                case FrameParamKeyType.PressureFront:
                    return new FrameParamDef(FrameParamKeyType.PressureFront, nameof(FrameParamKeyType.PressureFront),
                                          targetType,
                                          "Pressure at front of Drift Tube");

                case FrameParamKeyType.PressureBack:
                    return new FrameParamDef(FrameParamKeyType.PressureBack, nameof(FrameParamKeyType.PressureBack),
                                          targetType,
                                          "Pressure at back of Drift Tube");

                case FrameParamKeyType.HighPressureFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.HighPressureFunnelPressure,
                                          nameof(FrameParamKeyType.HighPressureFunnelPressure), targetType,
                                          "High pressure funnel pressure");

                case FrameParamKeyType.IonFunnelTrapPressure:
                    return new FrameParamDef(FrameParamKeyType.IonFunnelTrapPressure,
                                          nameof(FrameParamKeyType.IonFunnelTrapPressure), targetType,
                                          "Ion funnel trap pressure");

                case FrameParamKeyType.RearIonFunnelPressure:
                    return new FrameParamDef(FrameParamKeyType.RearIonFunnelPressure,
                                          nameof(FrameParamKeyType.RearIonFunnelPressure), targetType,
                                          "Rear ion funnel pressure");

                case FrameParamKeyType.QuadrupolePressure:
                    return new FrameParamDef(FrameParamKeyType.QuadrupolePressure,
                                          nameof(FrameParamKeyType.QuadrupolePressure), targetType,
                                          "Quadrupole pressure");

                case FrameParamKeyType.ESIVoltage:
                    return new FrameParamDef(FrameParamKeyType.ESIVoltage, nameof(FrameParamKeyType.ESIVoltage), targetType,
                                          "ESI voltage");

                case FrameParamKeyType.FloatVoltage:
                    return new FrameParamDef(FrameParamKeyType.FloatVoltage, nameof(FrameParamKeyType.FloatVoltage),
                                          targetType,
                                          "Float voltage");

                case FrameParamKeyType.FragmentationProfile:
                    return new FrameParamDef(FrameParamKeyType.FragmentationProfile,
                                          nameof(FrameParamKeyType.FragmentationProfile), targetType,
                                          "Voltage profile used in fragmentation (array of doubles, converted to an array of bytes, then stored as a Base 64 encoded string)");

                case FrameParamKeyType.ScanNumFirst:
                    return new FrameParamDef(FrameParamKeyType.ScanNumFirst, nameof(FrameParamKeyType.ScanNumFirst),
                                          targetType,
                                          "First scan");

                case FrameParamKeyType.ScanNumLast:
                    return new FrameParamDef(FrameParamKeyType.ScanNumLast, nameof(FrameParamKeyType.ScanNumLast),
                                          targetType,
                                          "Last scan");

                case FrameParamKeyType.PressureUnits:
                    return new FrameParamDef(FrameParamKeyType.PressureUnits, nameof(FrameParamKeyType.PressureUnits),
                                          targetType,
                                          "Units for pressure");

                default:
                    throw new ArgumentOutOfRangeException(nameof(paramType), "Unrecognized frame param enum for paramType: " + (int)paramType);
            }
        }
    }
}
