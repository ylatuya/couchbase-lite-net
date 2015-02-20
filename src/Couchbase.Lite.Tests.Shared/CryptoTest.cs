using System;
using NUnit.Framework;
using Couchbase.Lite.Store;
using System.Text;
using System.Linq;
using System.Collections.Generic;

namespace Couchbase.Lite
{
    public class CryptoTest : LiteTestCase
    {
        private const string Tag = "CryptoTest";
        private const string plaintext = "Jim Borden wants to encrypt something.";

        [Test]
        public void TestCryptoRoundTrip() {
            using(var key = new SymmetricKey("password")) {
                var plainBytes = Encoding.UTF8.GetBytes(plaintext);
                var cipherBytes = key.EncryptData(plainBytes);
                var decryptedBytes = key.DecryptData(cipherBytes);
                Assert.AreEqual(plainBytes, decryptedBytes);
            }
        }

        [Test]
        public void TestCryptoIncremental() {
            using(var key = new SymmetricKey()) {
                var plainBytes = Encoding.UTF8.GetBytes(plaintext);
                var block1 = plainBytes.Take(10).ToArray();
                var block2 = plainBytes.Skip(10).Take(10).ToArray();
                var block3 = plainBytes.Skip(20).ToArray();
                var cryptor = key.CreateEncryptor();

                //Shorter than block, shorter than block, longer than block
                var buffer = new List<byte>();
                buffer.AddRange(cryptor(block1));
                buffer.AddRange(cryptor(block2));
                buffer.AddRange(cryptor(block3));
                buffer.AddRange(cryptor(null));
                var result = buffer.ToArray();

                var decryptedBytes = key.DecryptData(result);
                Assert.AreEqual(plainBytes, decryptedBytes);

                //Longer than block
                buffer = new List<byte>();
                cryptor = key.CreateEncryptor();
                buffer.AddRange(cryptor(plainBytes));
                buffer.AddRange(cryptor(null));
                result = buffer.ToArray();

                decryptedBytes = key.DecryptData(result);
                Assert.AreEqual(plainBytes, decryptedBytes);

                //Shorter than block
                buffer = new List<byte>();
                cryptor = key.CreateEncryptor();
                buffer.AddRange(cryptor(new byte[] { 1 }));
                buffer.AddRange(cryptor(null));
                result = buffer.ToArray();

                decryptedBytes = key.DecryptData(result);
                Assert.AreEqual(decryptedBytes[0], 1);

                //Longer than block, shorter than block
                buffer = new List<byte>();
                cryptor = key.CreateEncryptor();
                block1 = plainBytes.Take(32).ToArray();
                block2 = plainBytes.Skip(32).ToArray();
                buffer.AddRange(cryptor(block1));
                buffer.AddRange(cryptor(block2));
                buffer.AddRange(cryptor(null));
                result = buffer.ToArray();

                decryptedBytes = key.DecryptData(result);
                Assert.AreEqual(plainBytes, decryptedBytes);
            }
        }
    }
}

