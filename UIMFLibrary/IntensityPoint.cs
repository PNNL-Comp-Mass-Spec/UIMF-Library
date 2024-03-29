﻿namespace UIMFLibrary
{
    using System;

    /// <summary>
    /// Defines the IntensityPoint type.
    /// </summary>
    public class IntensityPoint : IComparable<IntensityPoint>
    {
        // Ignore Spelling: ims

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="IntensityPoint"/> class.
        /// </summary>
        /// <param name="scanLc">
        /// LC scan (aka frame number)
        /// </param>
        /// <param name="scanIms">
        /// IMS scan
        /// </param>
        /// <param name="intensity">
        /// Intensity
        /// </param>
        public IntensityPoint(int scanLc, int scanIms, double intensity)
        {
            ScanLc = scanLc;
            ScanIms = scanIms;
            Intensity = intensity;
            IsSaturated = false;
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Gets or sets the intensity.
        /// </summary>
        public double Intensity { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether is saturated.
        /// </summary>
        public bool IsSaturated { get; set; }

        /// <summary>
        /// Gets the IMS scan
        /// </summary>
        public int ScanIms { get; }

        /// <summary>
        /// Gets the LC scan number (aka frame number)
        /// </summary>
        public int ScanLc { get; }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Overload the equals operator
        /// </summary>
        /// <param name="left">
        /// Left point
        /// </param>
        /// <param name="right">
        /// Right point
        /// </param>
        /// <returns>
        /// True if the points are equivalent
        /// </returns>
        public static bool operator ==(IntensityPoint left, IntensityPoint right)
        {
            return Equals(left, right);
        }

        /// <summary>
        /// Overload the not equals operator
        /// </summary>
        /// <param name="left">
        /// Left point
        /// </param>
        /// <param name="right">
        /// Right point
        /// </param>
        /// <returns>
        /// True if the points are not equivalent
        /// </returns>
        public static bool operator !=(IntensityPoint left, IntensityPoint right)
        {
            return !Equals(left, right);
        }

        /// <summary>
        /// Compare this point to another one
        /// </summary>
        /// <param name="other">
        /// Comparison point
        /// </param>
        /// <returns>
        /// Comparison result<see cref="int"/>.
        /// </returns>
        public int CompareTo(IntensityPoint other)
        {
            return ScanLc != other.ScanLc ? ScanLc.CompareTo(other.ScanLc) : ScanIms.CompareTo(other.ScanIms);
        }

        /// <summary>
        /// Check whether this point equals another point
        /// </summary>
        /// <param name="other">
        /// Comparison point
        /// </param>
        /// <returns>
        /// True if the objects are equal<see cref="bool"/>.
        /// </returns>
        public bool Equals(IntensityPoint other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return other.ScanLc == ScanLc && other.ScanIms == ScanIms;
        }

        /// <summary>
        /// Check whether this point equals another point (as an object)
        /// </summary>
        /// <param name="obj">
        /// Comparison object
        /// </param>
        /// <returns>
        /// True if the comparison object is an equivalent IntensityPoint<see cref="bool"/>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != typeof(IntensityPoint))
            {
                return false;
            }

            return Equals((IntensityPoint)obj);
        }

        /// <summary>
        /// Get hash code for this point
        /// </summary>
        /// <returns>
        /// Hash code for this instance<see cref="int"/>.
        /// </returns>
        public override int GetHashCode()
        {
            // Overflow is fine, just wrap
            unchecked
            {
                // Compute the hash code using a bitwise exclusive between ScanLc and ScanIMS
                // This method effectively generates a wide distribution of hash codes
                return (ScanLc * 397) ^ ScanIms;

                // Alternative method from http://stackoverflow.com/questions/263400/what-is-the-best-algorithm-for-an-overridden-system-object-gethashcode
                // This method produces a comparable distribution of hash codes
                //
                // int hash = 17;
                // hash = hash * 397 + this.ScanLc.GetHashCode();
                // hash = hash * 397 + this.ScanIms.GetHashCode();
                // return hash;
            }
        }

        #endregion
    }
}