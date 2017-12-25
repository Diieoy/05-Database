using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Diagnostics;

namespace Database
{
    class Program
    {
        const string connectionString = @"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=""..\..\..\Db.mdb""";

        static void Main(string[] args)
        {
            OleDbConnection con = new OleDbConnection(connectionString);
            Queue<int> queue = new Queue<int>();
            OleDbCommand command = null;
            OleDbDataReader reader = null;          

            try
            {
                //1
                con.Open();
                command = con.CreateCommand();
                try
                {   
                    command.CommandText = "SELECT int(ABS(x1 - x2) + 0.5) AS len, Count(*) AS num FROM Coordinates GROUP BY int(ABS(x1 - x2) + 0.5) ORDER BY int(ABS(x1 - x2) + 0.5) ASC";                   
                    reader = command.ExecuteReader();                   
                    
                    while (reader.Read())
                    {
                        queue.Enqueue((int)reader.GetDouble(0));
                        queue.Enqueue(reader.GetInt32(1));
                    }
                }
                catch (IndexOutOfRangeException e)
                {
                    StackTrace st = new StackTrace(e, true);
                    StackFrame frame = st.GetFrame(st.FrameCount - 1);

                    throw new IndexOutOfRangeException("Неверный индекс столбца, строка " + frame.GetFileLineNumber());
                }
                catch (OleDbException e)
                {
                    StackTrace st = new StackTrace(e, true);
                    StackFrame frame = st.GetFrame(st.FrameCount - 1);
                    
                    throw new IndexOutOfRangeException("Ошибка при создании команды (SQL запрос неверный), строка " + frame.GetFileLineNumber());
                }
                finally
                {       
                    if(reader != null)
                    {
                        reader.Close();
                    }                   
                }

                //2
                try
                {
                    command.CommandText = "DELETE * FROM Frequencies";
                    command.ExecuteNonQuery();
                  
                    command.CommandText = "INSERT INTO Frequencies(len, num) VALUES(@Len, @Num)";

                    command.Parameters.Add(new OleDbParameter("@Len", OleDbType.Integer));
                    command.Parameters.Add(new OleDbParameter("@Num", OleDbType.Integer));

                    while(queue.Count != 0)
                    {
                        command.Parameters["@Len"].Value = queue.Dequeue();
                        command.Parameters["@Num"].Value = queue.Dequeue();
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = "SELECT * FROM Frequencies";
                    reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        Console.WriteLine(reader["len"] + ";" + reader["num"]);
                    }
                }
                catch (IndexOutOfRangeException e)
                {
                    StackTrace st = new StackTrace(e, true);
                    StackFrame frame = st.GetFrame(st.FrameCount - 1);

                    throw new IndexOutOfRangeException("Неверное имя столбца, строка " + frame.GetFileLineNumber());
                }
                catch (OleDbException e)
                {
                    StackTrace st = new StackTrace(e, true);
                    StackFrame frame = st.GetFrame(st.FrameCount - 1);

                    throw new IndexOutOfRangeException("Ошибка при создании команды (SQL запрос неверный), строка " + frame.GetFileLineNumber());
                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                    }
                }

                //3
                try
                {
                    command.CommandText = "SELECT len, num FROM Frequencies WHERE len > num";
                    reader = command.ExecuteReader();

                    Console.WriteLine();
                    while (reader.Read())
                    {
                        Console.WriteLine(reader["len"] + ";" + reader["num"]);
                    }
                }
                catch (IndexOutOfRangeException e)
                {
                    StackTrace st = new StackTrace(e, true);
                    StackFrame frame = st.GetFrame(st.FrameCount - 1);

                    throw new IndexOutOfRangeException("Неверное имя столбца, строка " + frame.GetFileLineNumber());
                }
                catch (OleDbException e)
                {
                    StackTrace st = new StackTrace(e, true);
                    StackFrame frame = st.GetFrame(st.FrameCount - 1);

                    throw new IndexOutOfRangeException("Ошибка при создании команды (SQL запрос неверный), строка " + frame.GetFileLineNumber());
                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                    }
                }
            }
            catch (OleDbException e)
            {                
                Console.WriteLine(e.Message);
            }
            catch (IndexOutOfRangeException e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                con.Close();
            }

            Console.Read();
        }
    }
}
