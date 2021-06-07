using System;

namespace UIMFLibrary
{
    /// <summary>
    /// Units of Pressure
    /// </summary>
    public enum PressureUnits
    {
        /// <summary>
        /// Torr
        /// </summary>
        Torr = 0,

        /// <summary>
        /// MilliTorr
        /// </summary>
        MilliTorr = 1
    }

    /// <summary>
    /// Units of Temperature
    /// </summary>
    /// <remarks>FrameParams AmbientTemperature and DriftTubeTemperature are assumed to be in Celsius</remarks>
    public enum TemperatureUnits
    {
        /// <summary>
        /// Celsius
        /// </summary>
        Celsius = 0,

        /// <summary>
        /// Kelvin
        /// </summary>
        [Obsolete("No fields in this UIMFLibrary track temperature in Kelvin")]
        Kelvin = 1
    }
}
