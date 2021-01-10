<html>
<head>
    <title>spring boot form submit example</title>
</head>
<body>
<h1 align='center' style="font-family:Cambria; color:Black;">Linked Data: Question Answering System</h2>
<p class="aligncenter">
<img class="logo" src="ASULogo.png" alt="ASU Logo">
<h2 align='center'> Answer: </>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<table align='center'>
  <c:forEach items="${results}" var="item">
    <tr align='center'>
      <td align='center'><c:out value="${item}" /></td>
    </tr>
  </c:forEach>
</table>
</p>
</body>
<style>
.aligncenter {
    text-align: center;
}
</style>
</html>

