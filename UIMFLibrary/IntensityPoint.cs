// --------------------------------------------------------------------------------------------------------------------
// <copyright file="IntensityPoint.cs" company="PNNL">
//   
// </copyright>
// <summary>
//   Defines the IntensityPoint type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary
{
	using System;

	/// <summary>
	/// TODO The intensity point.
	/// </summary>
	public class IntensityPoint : IComparable<IntensityPoint>
	{
		/// <summary>
		/// Gets the scan lc.
		/// </summary>
		public int ScanLc { get; private set; }

		/// <summary>
		/// Gets the scan ims.
		/// </summary>
		public int ScanIms { get; private set; }

		/// <summary>
		/// Gets or sets the intensity.
		/// </summary>
		public double Intensity { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether is saturated.
		/// </summary>
		public bool IsSaturated { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="IntensityPoint"/> class.
		/// </summary>
		/// <param name="scanLc">
		/// TODO The scan lc.
		/// </param>
		/// <param name="scanIms">
		/// TODO The scan ims.
		/// </param>
		/// <param name="intensity">
		/// TODO The intensity.
		/// </param>
		public IntensityPoint(int scanLc, int scanIms, double intensity)
		{
			ScanLc = scanLc;
			ScanIms = scanIms;
			Intensity = intensity;
			IsSaturated = false;
		}

		/// <summary>
		/// TODO The compare to.
		/// </summary>
		/// <param name="other">
		/// TODO The other.
		/// </param>
		/// <returns>
		/// The <see cref="int"/>.
		/// </returns>
		public int CompareTo(IntensityPoint other)
		{
			return this.ScanLc != other.ScanLc ? this.ScanLc.CompareTo(other.ScanLc) : this.ScanIms.CompareTo(other.ScanIms);
		}

		/// <summary>
		/// TODO The equals.
		/// </summary>
		/// <param name="other">
		/// TODO The other.
		/// </param>
		/// <returns>
		/// The <see cref="bool"/>.
		/// </returns>
		public bool Equals(IntensityPoint other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return other.ScanLc == ScanLc && other.ScanIms == ScanIms;
		}

		/// <summary>
		/// TODO The equals.
		/// </summary>
		/// <param name="obj">
		/// TODO The obj.
		/// </param>
		/// <returns>
		/// The <see cref="bool"/>.
		/// </returns>
		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof (IntensityPoint)) return false;
			return Equals((IntensityPoint) obj);
		}

		/// <summary>
		/// TODO The get hash code.
		/// </summary>
		/// <returns>
		/// The <see cref="int"/>.
		/// </returns>
		public override int GetHashCode()
		{
			unchecked
			{
				return (ScanLc*397) ^ ScanIms;
			}
		}

		/// <summary>
		/// TODO The ==.
		/// </summary>
		/// <param name="left">
		/// TODO The left.
		/// </param>
		/// <param name="right">
		/// TODO The right.
		/// </param>
		/// <returns>
		/// </returns>
		public static bool operator ==(IntensityPoint left, IntensityPoint right)
		{
			return Equals(left, right);
		}

		/// <summary>
		/// TODO The !=.
		/// </summary>
		/// <param name="left">
		/// TODO The left.
		/// </param>
		/// <param name="right">
		/// TODO The right.
		/// </param>
		/// <returns>
		/// </returns>
		public static bool operator !=(IntensityPoint left, IntensityPoint right)
		{
			return !Equals(left, right);
		}
	}
}
