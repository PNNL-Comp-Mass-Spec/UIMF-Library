using System;
using System.Collections.Generic;
using System.Globalization;

namespace UIMFLibrary
{
    /// <summary>
    /// Container for a set of frame parameters
    /// </summary>
    public class FrameParams
    {
        #region Structures

        /// <summary>
        /// Mass Calibration Coefficients
        /// </summary>
        public struct MassCalibrationCoefficientsType
        {
            /// <summary>
            /// Calibration Coefficient a2
            /// </summary>
            public double a2;

            /// <summary>
            /// Calibration Coefficient b2
            /// </summary>
            public double b2;

            /// <summary>
            /// Calibration Coefficient c2
            /// </summary>
            public double c2;

            /// <summary>
            /// Calibration Coefficient d2
            /// </summary>
            public double d2;

            /// <summary>
            /// Calibration Coefficient e2
            /// </summary>
            public double e2;

            /// <summary>
            /// Calibration Coefficient f2
            /// </summary>
            public double f2;
        }

        #endregion

        #region Member Variables

        /// <summary>
        /// Frame parameters dictionary
        /// </summary>
        /// <remarks>Key is parameter type; value is the frame parameter container (<see cref="FrameParam"/> class)</remarks>
        private readonly Dictionary<FrameParamKeyType, FrameParam> mFrameParameters;

        /// <summary>
        /// Mass calibration coefficients are cached to allow for fast lookup via external classes
        /// </summary>
        private MassCalibrationCoefficientsType mCachedMassCalibrationCoefficients;

        #endregion

        #region Properties

        /// <summary>
        /// Frame parameters dictionary
        /// </summary>
        /// <remarks>Key is parameter type; value is the frame parameter container (<see cref="FrameParam"/> class)</remarks>
        public Dictionary<FrameParamKeyType, FrameParam> Values
        {
            get { return mFrameParameters; }
        }

        /// <summary>
        /// Calibration slope
        /// </summary>
        /// <remarks>Returns 0 if not defined</remarks>
        public double CalibrationSlope
        {
            get
            {
                if (!HasParameter(FrameParamKeyType.CalibrationSlope))
                    return 0;

                return GetValueDouble(FrameParamKeyType.CalibrationSlope);
            }
        }

        /// <summary>
        /// Calibration intercept
        /// </summary>
        /// <remarks>Returns 0 if not defined</remarks>
        public double CalibrationIntercept
        {
            get
            {
                if (!HasParameter(FrameParamKeyType.CalibrationIntercept))
                    return 0;

                return GetValueDouble(FrameParamKeyType.CalibrationIntercept);
            }
        }

        /// <summary>
        /// Frame type
        /// </summary>
        /// <remarks>Returns MS1 if not defined</remarks>
        public DataReader.FrameType FrameType
        {
            get
            {
                if (!HasParameter(FrameParamKeyType.FrameType))
                    return DataReader.FrameType.MS1;

                var frameType = GetValueInt32(FrameParamKeyType.FrameType);
                if (frameType == 0)
                {
                    // This is an older UIMF file where the MS1 frames were labeled as 0
                    return DataReader.FrameType.MS1;
                }

                return (DataReader.FrameType)frameType;
            }
        }

        /// <summary>
        /// Mass calibration coefficients
        /// </summary>
        /// <remarks>Provided for quick reference to avoid having to access the dictionary and convert from string to double</remarks>
        public MassCalibrationCoefficientsType MassCalibrationCoefficients
        {
            get
            {
                return mCachedMassCalibrationCoefficients;
            }
        }

        /// <summary>
        /// Scans per frame
        /// </summary>
        /// <remarks>Returns 0 if not defined</remarks>
        public int Scans
        {
            get
            {
                if (!HasParameter(FrameParamKeyType.Scans))
                    return 0;

                return GetValueInt32(FrameParamKeyType.Scans);
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public FrameParams()
        {
            mFrameParameters = new Dictionary<FrameParamKeyType, FrameParam>();
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (double)</param>
        public FrameParams AddUpdateValue(FrameParamKeyType paramType, double value)
        {
            return AddUpdateValue(paramType, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (int)</param>
        public FrameParams AddUpdateValue(FrameParamKeyType paramType, int value)
        {
            return AddUpdateValue(paramType, value.ToString(CultureInfo.InvariantCulture));            
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="value">Value (string)</param>
        public FrameParams AddUpdateValue(FrameParamKeyType paramType, string value)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                paramEntry.Value = value;
            }
            else
            {
                paramEntry = new FrameParam(FrameParamUtilities.GetParamDefByType(paramType), value);
                mFrameParameters.Add(paramType, paramEntry);
            }

            UpdateCachedParam(paramType, value);
            
            return this;
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramDef">Frame parameter definition (<see cref="FrameParamDef"/> class)</param>
        /// <param name="value">Value (double)</param>
        public FrameParams AddUpdateValue(FrameParamDef paramDef, double value)
        {
            return AddUpdateValue(paramDef, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramDef">Frame parameter definition (<see cref="FrameParamDef"/> class)</param>
        /// <param name="value">Value (int)</param>
        public FrameParams AddUpdateValue(FrameParamDef paramDef, int value)
        {
            return AddUpdateValue(paramDef, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a parameter's value
        /// </summary>
        /// <param name="paramDef">Frame parameter definition (<see cref="FrameParamDef"/> class)</param>
        /// <param name="value">Value (string)</param>
        public FrameParams AddUpdateValue(FrameParamDef paramDef, string value)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramDef.ParamType, out paramEntry))
            {
                paramEntry.Value = value;
            }
            else
            {
                paramEntry = new FrameParam(paramDef, value);
                mFrameParameters.Add(paramDef.ParamType, paramEntry);
            }

            return this;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (string)</returns>
        /// <remarks>Returns an empty string if not defined</remarks>
        public string GetValue(FrameParamKeyType paramType)
        {
            return GetValue(paramType, string.Empty);
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (string)</returns>
        public string GetValue(FrameParamKeyType paramType, string valueIfMissing)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                return paramEntry.Value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (double)</returns>
        /// <remarks>Returns 0 if not defined</remarks>
        public double GetValueDouble(FrameParamKeyType paramType)
        {
            return GetValueDouble(paramType, 0.0);
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (double)</returns>
        public double GetValueDouble(FrameParamKeyType paramType, double valueIfMissing)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                double result;
                if (Double.TryParse(paramEntry.Value, out result))
                    return result;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>Value (int)</returns>
        /// <remarks>Returns 0 if not defined</remarks>
        public int GetValueInt32(FrameParamKeyType paramType)
        {
            return GetValueInt32(paramType, 0);
        }

        /// <summary>
        /// Get the value for a parameter
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not defined</param>
        /// <returns>Value (int)</returns>
        public int GetValueInt32(FrameParamKeyType paramType, int valueIfMissing)
        {
            FrameParam paramEntry;
            if (mFrameParameters.TryGetValue(paramType, out paramEntry))
            {
                int result;
                if (Int32.TryParse(paramEntry.Value, out result))
                    return result;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Lookup whether or not a frame parameter is defined
        /// </summary>
        /// <param name="paramType">Parameter type</param>
        /// <returns>True if defined, otherwise false</returns>
        public bool HasParameter(FrameParamKeyType paramType)
        {
            return mFrameParameters.ContainsKey(paramType);
        }

        private void UpdateCachedParam(FrameParamKeyType paramType, string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return;

            double numericValue;
            if (!double.TryParse(value, out numericValue))
                return;

            // Update cached member variables
            // At present, the only cached values are mass calibration coefficients

            switch (paramType)
            {
                case FrameParamKeyType.MassCalibrationCoefficienta2:
                    mCachedMassCalibrationCoefficients.a2 = numericValue;
                    break;
                case FrameParamKeyType.MassCalibrationCoefficientb2:
                    mCachedMassCalibrationCoefficients.b2 = numericValue;
                    break;
                case FrameParamKeyType.MassCalibrationCoefficientc2:
                    mCachedMassCalibrationCoefficients.c2 = numericValue;
                    break;
                case FrameParamKeyType.MassCalibrationCoefficientd2:
                    mCachedMassCalibrationCoefficients.d2 = numericValue;
                    break;
                case FrameParamKeyType.MassCalibrationCoefficiente2:
                    mCachedMassCalibrationCoefficients.e2 = numericValue;
                    break;
                case FrameParamKeyType.MassCalibrationCoefficientf2:
                    mCachedMassCalibrationCoefficients.f2 = numericValue;
                    break;
            }
        }
    }
}
