import java.sql.Timestamp

case class Login(name: String, time: String, status: String)

case class Login2(name: String, time: Timestamp, status: String)