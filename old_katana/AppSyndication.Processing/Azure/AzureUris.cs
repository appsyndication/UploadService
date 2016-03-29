﻿using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace FireGiant.AppSyndication.Processing.Azure
{
    public static class AzureUris
    {
        private readonly static Regex Fix = new Regex(@"[^A-Za-z0-9]+", RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture | RegexOptions.Singleline);

        //private static string _containerUri;

        public static string AzureSafeId(string id)
        {
            return Fix.Replace(id, "-").Trim('-').ToLowerInvariant();
        }

        //public static string AbsoluteUriInContainer(string blobRelativeUri)
        //{
        //    return _containerUri + "/" + blobRelativeUri;
        //}

        //public static string JsonTagBlobInTagContainer(string blobName)
        //{
        //    return _containerUri + "/" + blobName + ".jsontag";
        //}

        //public static string SwidTagBlobInTagContainer(string blobName)
        //{
        //    return _containerUri + "/" + blobName + ".swidtag";
        //}

        //public static string JsonActiveTagBlobRelativeUri(string sourceBlobName, string tagBlobName)
        //{
        //    return sourceBlobName + "/" + tagBlobName + ".jsontag";
        //}

        //public static string JsonActiveTagBlobRelativeUri(string tagBlobName)
        //{
        //    return tagBlobName + ".jsontag";
        //}

        //public static string JsonTagVersionedBlobRelativeUri(string sourceBlobName, string tagBlobName, string version)
        //{
        //    return sourceBlobName + "/" + tagBlobName + "/v" + version + ".jsontag";
        //}

        public static string JsonTagVersionedBlobRelativeUri(string tagBlobName, string version)
        {
            return string.Concat(tagBlobName, "/v", version, ".json.swidtag").ToLowerInvariant();
        }

        public static string XmlTagVersionedBlobRelativeUri(string tagBlobName, string version)
        {
            return string.Concat(tagBlobName, "/v", version, ".xml.swidtag").ToLowerInvariant();
        }

        public static string CalculateKey(params string[] args)
        {
            var key = String.Join("|", args);

            var bytes = Encoding.UTF8.GetBytes(key);

            using (var md5 = MD5.Create())
            {
                var hash = md5.ComputeHash(bytes);

                //return BitConverter.ToString(hash).Replace("-", String.Empty).ToLowerInvariant();
                return AzureUris.Base32Modified(hash);
            }
        }

        /// <summary>
        /// Size of the regular byte in bits
        /// </summary>
        private const int InByteSize = 8;

        /// <summary>
        /// Size of converted byte in bits
        /// </summary>
        private const int OutByteSize = 5;

        /// <summary>
        /// zBase32 alphabet modified to reduce chance of profanity.
        /// </summary>
        private const string Base32Alphabet = "bcdefghjklmnpqrstvwzyz1234567890";
        //private const string Base32Alphabet = "ybndrfg8ejkmcpqx2t1vwlsza345h769";

        public static string Base32Modified(byte[] bytes)
        {
            var builder = new StringBuilder(bytes.Length * InByteSize / OutByteSize);
            var bytesPosition = 0;
            var bytesSubPosition = 0;
            var outputBase32Byte = 0;
            var outputBase32BytePosition = 0;

            while (bytesPosition < bytes.Length)
            {
                var bitsAvailableInByte = Math.Min(InByteSize - bytesSubPosition, OutByteSize - outputBase32BytePosition);

                outputBase32Byte <<= bitsAvailableInByte;

                outputBase32Byte |= (byte)(bytes[bytesPosition] >> (InByteSize - (bytesSubPosition + bitsAvailableInByte)));

                bytesSubPosition += bitsAvailableInByte;

                if (bytesSubPosition >= InByteSize)
                {
                    bytesPosition++;
                    bytesSubPosition = 0;
                }

                outputBase32BytePosition += bitsAvailableInByte;

                if (outputBase32BytePosition >= OutByteSize)
                {
                    outputBase32Byte &= 0x1F;  // 0x1F = 00011111 in binary

                    builder.Append(Base32Alphabet[outputBase32Byte]);

                    outputBase32BytePosition = 0;
                }
            }

            // Check if we have a remainder
            if (outputBase32BytePosition > 0)
            {
                // Move to the right bits
                outputBase32Byte <<= (OutByteSize - outputBase32BytePosition);

                // Drop the overflow bits
                outputBase32Byte &= 0x1F;  // 0x1F = 00011111 in binary

                // Add current Base32 byte and convert it to character
                builder.Append(Base32Alphabet[outputBase32Byte]);
            }

            return builder.ToString();
        }
    }
}