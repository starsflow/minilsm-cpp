/*
 * @Author: lxc
 * @Date: 2024-03-30 22:00:47
 * @Description: 
 */
#include <folly/concurrency/ConcurrentHashMap.h>
#include <string>
#include <iostream>

class Student 
{
public:
    Student(std::string name, int id, std::string email)
        : m_name(name), m_id(id), m_email(email)
    {}

    void printSelf() const 
    {
        std::cout << "name: " << m_name << " "
            << "id: " << m_id << " "
            << "email: " << m_email << std::endl;
    }

private:
    std::string m_name;
    int m_id;
    std::string m_email;
};

int main() 
{
    folly::ConcurrentHashMap<std::string, Student> students;
    students.insert("Tom", Student("Tom", 1, "tom@gmail.com"));
    students.insert("Lilly", Student("Lilly", 2, "lilly@gmail.com"));

    for (const auto& st : students) 
    {
        st.second.printSelf();
    }
    
    return 0;
}