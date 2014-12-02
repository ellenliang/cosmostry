<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Home.aspx.cs" Inherits="WebApplication1.Home" %>

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title></title>
</head>
<body>
    <form id="form1" runat="server">
    <div>
    
        <asp:Button ID="Button1" runat="server" OnClick="Button1_Click" Text="Run iScope Query" />
        <br />
    
    </div>
        <p>
            iScope web app&nbsp;</p>
        <p>
            <asp:TextBox ID="TextBox1" runat="server" Height="636px" Width="1339px" TextMode="MultiLine"></asp:TextBox>
        </p>
    </form>
</body>
</html>
