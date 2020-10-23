#include<fcntl.h>
#include<unistd.h>
#include<error.h>
#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include"CLTable.h"
#include<iostream>
#include<algorithm>
#define TABLE_FILE_NAME "CLTable"
#define TABLE_CONTRL_NAME "CLTableRows"
// #define BUFFER_SIZE_TABLE_FILE 4096
#define ROW_LENGTH (100*sizeof(int64_t))
#define BUFFER_ROW 5

CLTable* CLTable::m_pTable = 0;
pthread_mutex_t *CLTable::m_pMutexForCreatator = CLTable::InitializeMutex();

pthread_mutex_t *CLTable::InitializeMutex(){
    pthread_mutex_t *p = new pthread_mutex_t;

    if(pthread_mutex_init(p,0)!=0){
        delete p;
        return 0;
    }
    return p;
}

CLTable::CLTable(){
    m_Fd = open(TABLE_FILE_NAME, O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
    int64_t temp[100] = {0};
    if(lseek(m_Fd,0,SEEK_END)==0){
        write(m_Fd, temp, ROW_LENGTH);
        m_nRows = temp[0];
        // std::cout << temp[0] << "  write  " << 1 << std::endl;
    }
    else {
        lseek(m_Fd, 0, SEEK_SET);
        read(m_Fd,temp,ROW_LENGTH);
        m_nRows = temp[0];
        // std::cout << temp[0] <<"  read  "<< 2 << std::endl;
    }
    m_nReaderRows = m_nRows;
    if(m_nReaderRows!=0){
        int64_t *m_pReader = (int64_t *)malloc(ROW_LENGTH * m_nReaderRows);
        lseek(m_Fd, ROW_LENGTH, SEEK_SET);
        read(m_Fd,m_pReader,ROW_LENGTH*m_nReaderRows);
        // std::cout << *(m_pReader+100) << "  read2  " << 3 << std::endl;
    }
    else{
        int64_t *m_pReader = (int64_t *)malloc(ROW_LENGTH);
    }
    
    std::cout << "There are "<<m_nRows<<" rows in table!"<<std::endl;
    std::cout << "There are " << lseek(m_Fd,0,SEEK_END)/ROW_LENGTH << " rows in file!" << std::endl;
    if(m_Fd == -1)
        throw "In CLTable::CLTable(), open error";

    m_pTableBuffer = new int64_t[ROW_LENGTH*BUFFER_ROW];
    m_nUsedRowsForBuffer = 0;

    m_bFlagForProcessExit = false;

    m_pMutexForWritingTable = new pthread_mutex_t;
    if (pthread_mutex_init(m_pMutexForWritingTable,0)!=0)
    {
        delete m_pMutexForWritingTable;
        delete [] m_pTableBuffer;
        close(m_Fd);

        throw "In CLTable::CLTable(), pthread_mutex_init for Writer error";

    }
    m_pMutexForReadingTable = new pthread_mutex_t;
    m_nReaders = 0;
    if (pthread_mutex_init(m_pMutexForReadingTable, 0) != 0)
    {
        delete m_pMutexForReadingTable;
        delete[] m_pTableBuffer;
        close(m_Fd);

        throw "In CLTable::CLTable(), pthread_mutex_init for Reader error";
    }
}




int CLTable::WriteRowMsg(const int64_t* pstrMsg){
    CLTable *pTable = CLTable::GetInstance();
    if(pTable ==0)
        return -1;
    int s = pTable->WriteRow(pstrMsg);
    return s;
}
int CLTable::ReadRowMsg()
{
    CLTable *pTable = CLTable::GetInstance();
    if (pTable == 0)
        return -1;
    int s = pTable->ReadRow();
    return s;
}

int CLTable::Flush(){
    if(pthread_mutex_lock(m_pMutexForWritingTable)!=0)
        return -1;
    try
    {
        if (m_nUsedRowsForBuffer == 0)
            throw 0;
        lseek(m_Fd,0,SEEK_END);
        if (write(m_Fd, m_pTableBuffer, ROW_LENGTH*m_nUsedRowsForBuffer) == -1)
            throw -1;
        m_nRows+=m_nUsedRowsForBuffer;
        m_nUsedRowsForBuffer = 0;
        throw 0;
    }
    catch(int &s)
    {
        if(pthread_mutex_unlock(m_pMutexForWritingTable)!=0)
            return -1;
        return s;
    }
}

int CLTable::WriteRow(const int64_t *pstrMsg)
{
    if(pstrMsg==0)
        return -1;
    if(m_nUsedRowsForBuffer<BUFFER_ROW){
        pthread_mutex_lock(m_pMutexForWritingTable);
        try
        {
            memcpy(m_pTableBuffer+(100*m_nUsedRowsForBuffer),pstrMsg,ROW_LENGTH);
            // std::cout << m_pTableBuffer << "     " << m_pTableBuffer+(100 * m_nUsedRowsForBuffer) << std::endl;
            m_nUsedRowsForBuffer++;
            if (m_nUsedRowsForBuffer == BUFFER_ROW)
            {
                // std::cout << *m_pTableBuffer << "     "<<*(m_pTableBuffer+1) <<"  1"<< std::endl;
                // std::cout << *(m_pTableBuffer + 100) << "     " << *(m_pTableBuffer + 101) << "  2" << std::endl;
                throw CLTable::Flush();
            }
            throw 0;
        }
        catch(int & s)
        {
            if(pthread_mutex_unlock(m_pMutexForWritingTable)!=0)
                return -1;
            return s;
        }
    }
    
    return -1;
}

int CLTable::ReadRow(){
    if(m_nReaderRows==m_nRows) return 0;
    if(m_nRows==0) return 0;
    
    pthread_mutex_lock(m_pMutexForReadingTable);
    std::cout << "m_pMutexForReadingTable locked!" << std::endl;
    if(m_nReaderRows==m_nRows){
        if (pthread_mutex_unlock(m_pMutexForReadingTable) != 0)
            return -1;
        std::cout << "m_pMutexForReadingTable unlocked!" << std::endl;
        return 0;
    }
    try
    {
        free(m_pReader);
        std::cout << "m_pReader free!" << std::endl;
        m_pReader =nullptr;
        m_pReader = (int64_t *)malloc(ROW_LENGTH * m_nRows);
        std::cout << "m_pReader malloc again!" << std::endl;
        lseek(m_Fd, ROW_LENGTH, SEEK_SET);
        read(m_Fd, m_pReader, ROW_LENGTH * m_nRows);
        std::cout << "m_pReader Read again!" << std::endl;
        int num = m_nRows-m_nReaderRows;
        throw num;
    }
    catch (int &s)
    {   
        m_nReaderRows=m_nRows;
        if (pthread_mutex_unlock(m_pMutexForReadingTable) != 0)
            return -1;
        std::cout << "m_pMutexForReadingTable unlocked!" << std::endl;
        return s;
    }
}

CLTable* CLTable::GetInstance(){
    if(m_pTable != 0)
        return m_pTable;

    if(m_pMutexForCreatator == 0)
        return 0;
    
    if(pthread_mutex_lock(m_pMutexForCreatator) !=0)
        return 0;

    if(m_pTable ==0){
        try
        {
            m_pTable = new CLTable;
        }
        catch(const char*)
        {
            pthread_mutex_unlock(m_pMutexForCreatator);
            return 0;
        }

        if(atexit(CLTable::OnProcessExit)!=0){
            m_pTable->m_bFlagForProcessExit =true;

            if(pthread_mutex_unlock(m_pMutexForCreatator)!=0)
                return 0;
        }
        else
        {
            if(pthread_mutex_unlock(m_pMutexForCreatator)!=0)
                return 0;
        }
        return m_pTable;        
    }
    if (pthread_mutex_unlock(m_pMutexForCreatator) != 0)
        return 0;

    return m_pTable;
}

void CLTable::OnProcessExit(){
    CLTable* pTable = CLTable::GetInstance();
    if(pTable !=0)
    {
        pthread_mutex_lock(pTable->m_pMutexForWritingTable);
        pTable->m_bFlagForProcessExit = true;
        pthread_mutex_unlock(pTable->m_pMutexForWritingTable);

        pTable->Flush();
    }
    if (lseek(pTable->m_Fd, 0, SEEK_SET) >= 0)
    {
        int64_t temp[100] = {0};
        temp[0] = pTable->m_nRows;

        write(pTable->m_Fd, temp, sizeof(int64_t) * 100);
        std::cout << "Before close the table, there are "<<pTable->m_nRows<<" rows in total."<<std::endl;
        std::cout << "Before close the table, there are " << lseek(pTable->m_Fd, 0, SEEK_END)/ROW_LENGTH << " rows in file." << std::endl;
    }
    delete pTable->m_pMutexForWritingTable;
    delete pTable->m_pMutexForReadingTable;
    delete[] pTable->m_pTableBuffer;
    close(pTable->m_Fd);
    std::cout << "OnProcessExit() has been execute!"<<std::endl;
}


int main(){
    
    CLTable * pTable = CLTable::GetInstance();
    for (int64_t i =0;i<500000;i++){
        int64_t tmp[100];
        std::fill(tmp,tmp+100,i);
        pTable->WriteRowMsg(tmp);
    }
    int num = pTable->ReadRowMsg();
    std::cout<<"Read more data : "<<num<<std::endl;
    // int64_t tmp[100] ={1};
    // pTable->WriteRowMsg(tmp);

    return 0;
}