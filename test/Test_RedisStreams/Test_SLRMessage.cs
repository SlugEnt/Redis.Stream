using NUnit.Framework;
using SlugEnt.SLRStreamProcessing;

namespace Test_RedisStreams;

[TestFixture]
public class Test_SLRMessage
{
    [Test]
    public void String_Property()
    {
        string propName  = "UserName";
        string propValue = "Fred Flinstone";

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        string retValue = message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }


    [Test]
    public void Int_Property()
    {
        string propName  = "UserAge";
        int    propValue = 54;

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        int retValue = (int)message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }



    [Test]
    public void Long_Property()
    {
        string propName  = "UserAge";
        long   propValue = long.MaxValue;

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        long retValue = (long)message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }



    [Test]
    public void UnsignedLong_Property()
    {
        string propName  = "UserAge";
        ulong  propValue = ulong.MaxValue;

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        ulong retValue = (ulong)message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }


    [Test]
    public void Short_Property()
    {
        string propName  = "UserAge";
        short  propValue = short.MaxValue;

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        short retValue = (short)message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }



    [Test]
    public void UnsignedInt_Property()
    {
        string propName  = "UserAge";
        uint   propValue = uint.MaxValue;

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        uint retValue = (uint)message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }



    [Test]
    public void Double_Property()
    {
        string propName  = "UserAge";
        double propValue = double.MaxValue;

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        double retValue = (double)message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }



    [Test]
    public void Single_Property()
    {
        string propName  = "UserAge";
        Single propValue = Single.MaxValue;

        SLRMessage message = new SLRMessage();
        message.AddProperty(propName, propValue);
        Single retValue = (Single)message.GetProperty(propName);
        Assert.AreEqual(propValue, retValue, "A10:");
    }


    [Test]
    public void Object_Property()
    {
        string     name     = "Fred Flinstone";
        string     password = "YabbaDabbaDoo";
        uint       age      = 54;
        UserSample user     = new(name, password, age);

        SLRMessage message = new SLRMessage();
        message.AddPropertyObject<UserSample>("User", user);

        UserSample desUser = message.GetPropertyObject<UserSample>("User");
        Assert.AreEqual(age, desUser.Age, "A10");
        Assert.AreEqual(name, desUser.Name, "A20");
        Assert.AreEqual(password, desUser.Password, "A30");
    }



    [Test]
    public void IsTextMessage()
    {
        string     text    = "hello";
        SLRMessage message = SLRMessage.CreateMessage(text);
        Assert.IsTrue(message.IsTextualMessage, "A10:");
        Assert.IsFalse(message.IsObjectEncodedMessage, "A20:");
        Assert.IsFalse(message.IsGenericMessage, "A30:");
    }


    [Test]
    public void IsObjectEncodedMessage()
    {
        string     name     = "Fred Flinstone";
        string     password = "YabbaDabbaDoo";
        uint       age      = 54;
        UserSample user     = new(name, password, age);

        SLRMessage message = SLRMessage.CreateMessage<UserSample>(user);
        Assert.IsFalse(message.IsTextualMessage, "A10:");
        Assert.IsTrue(message.IsObjectEncodedMessage, "A20:");
        Assert.IsFalse(message.IsGenericMessage, "A30:");

        UserSample desUser = message.GetMessageObject<UserSample>();
        Assert.AreEqual(age, desUser.Age, "A100");
        Assert.AreEqual(name, desUser.Name, "A120");
        Assert.AreEqual(password, desUser.Password, "A130");
    }


    [Test]
    public void IsGenericMessage()
    {
        SLRMessage message = new();
        Assert.IsFalse(message.IsTextualMessage, "A10:");
        Assert.IsFalse(message.IsObjectEncodedMessage, "A20:");
        Assert.IsTrue(message.IsGenericMessage, "A30:");
    }
}