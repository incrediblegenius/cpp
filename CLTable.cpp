#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "CLTable.h"
#include <iostream>
#include <algorithm>
#include <random>
#include <functional>


#define TABLE_FILE_NAME "CLTable"
#define ROW_LENGTH (100 * sizeof(int64_t))
#define BUFFER_ROW 500
#define NUM_THREADS 5

CLTable *CLTable::m_pTable = 0;
pthread_mutex_t *CLTable::m_pMutexForCreatator = CLTable::InitializeMutex();

pthread_mutex_t *CLTable::InitializeMutex()
{
    pthread_mutex_t *p = new pthread_mutex_t;

    if (pthread_mutex_init(p, 0) != 0)
    {
        delete p;
        return 0;
    }
    return p;
}

CLTable::CLTable()
{
    m_Fd = open(TABLE_FILE_NAME, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    int64_t temp[100] = {0};
    if (lseek(m_Fd, 0, SEEK_END) == 0)
    {
        write(m_Fd, temp, ROW_LENGTH);
        m_nRows = temp[0];
        // std::cout << temp[0] << "  write  " << 1 << std::endl;
    }
    else
    {
        lseek(m_Fd, 0, SEEK_SET);
        read(m_Fd, temp, ROW_LENGTH);
        m_nRows = temp[0];
        // std::cout << temp[0] <<"  read  "<< 2 << std::endl;
    }
    m_nReaderRows = m_nRows;
    if (m_nReaderRows != 0)
    {
        m_pReader = (int64_t *)malloc(ROW_LENGTH * m_nReaderRows);
        lseek(m_Fd, ROW_LENGTH, SEEK_SET);
        read(m_Fd, m_pReader, ROW_LENGTH * m_nReaderRows);
        // std::cout << m_nReaderRows<<"   "<<m_pReader[0] << std::endl;

        // std::cout << *(m_pReader+100) << "  read2  " << 3 << std::endl;
    }
    else
    {
        m_pReader = 0;
        // std::cout<<"m_pReader dis"<<std::endl;
    }

    std::cout << "There are " << m_nRows << " rows in table!" << std::endl;
    std::cout << "There are " << lseek(m_Fd, 0, SEEK_END) / ROW_LENGTH << " rows in file!" << std::endl;
    if (m_Fd == -1)
        throw "In CLTable::CLTable(), open error";

    m_pTableBuffer = new int64_t[ROW_LENGTH * BUFFER_ROW];
    m_nUsedRowsForBuffer = 0;

    m_bFlagForProcessExit = false;

    m_pMutexForWritingTable = new pthread_mutex_t;
    if (pthread_mutex_init(m_pMutexForWritingTable, 0) != 0)
    {
        delete m_pMutexForWritingTable;
        delete[] m_pTableBuffer;
        close(m_Fd);

        throw "In CLTable::CLTable(), pthread_mutex_init for Writer error";
    }
}

int CLTable::WriteRowMsg(const int64_t *pstrMsg)
{
    CLTable *pTable = CLTable::GetInstance();
    if (pTable == 0)
        return -1;
    if (pTable->m_nRows >= 1000000)
        return -2;
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

int64_t **CLTable::SearchFromTable(int attribute, int64_t low, int64_t high)
{
    CLTable *pTable = CLTable::GetInstance();
    if (attribute >= 100 || attribute < 0 || low > high)
        return 0;
    if ((pTable->ReadIndex(attribute)) == 0)
    {
        std::cout << "Normal Searching!" << std::endl;
        return pTable->Search(attribute, low, high);
    }
    std::cout << "Index Searching!" << std::endl;
    return pTable->SearchWithIndex(attribute, low, high);
}

int64_t **CLTable::Search(int attribute, int64_t low, int64_t high)
{
    // std::cout<<m_nReaderRows<<m_nRows<<std::endl;
    CLTable::ReadRowMsg();
    pthread_mutex_lock(m_pMutexForWritingTable);
    std::cout << "Normal Search locked!" << std::endl;
    int64_t **res = (int64_t **)malloc(sizeof(int64_t *) * 10);
    for (int i = 0; i < 10; i++)
    {
        res[i] = nullptr;
    }
    // std::cout<< "res has been malloced!"<<std::endl;

    try
    {
        int cnt = 0;
        int64_t *tmp;
        for (int64_t i = 0; i < m_nReaderRows; i++)
        {
            tmp = m_pReader + 100 * i;
            // std::cout << "i=" << i << "    m_nReaderRows=" << m_nReaderRows << std::endl;
            // std::cout << "m_pReader[0] = " << (m_pReader==0?0:1) << std::endl;
            if (tmp[attribute] >= low && tmp[attribute] <= high)
            {
                if(cnt ==0){
                std::cout << "Normal search success!" << std::endl;
            }
                res[cnt] = (int64_t *)malloc(ROW_LENGTH);
                memcpy(res[cnt], tmp, ROW_LENGTH);
                cnt++;
                
                if (cnt == 10)
                    break;
            }
        }
        // std::cout << "end m_nReaderRows" << std::endl;
        if (cnt < 10)
        {
            for (int i = 0; i < m_nUsedRowsForBuffer; i++)
            {
                int64_t *tmp = m_pTableBuffer + i;
                if (tmp[attribute] >= low && tmp[attribute] <= high)
                {
                    res[cnt] = (int64_t *)malloc(ROW_LENGTH);
                    memcpy(res[cnt], tmp, ROW_LENGTH);
                    cnt++;
                    std::cout << "cnt=" << cnt << std::endl;
                    if (cnt == 10)
                        break;
                }
            }
        }

        std::cout << "cnt = "<<cnt << std::endl;
        throw 0;
    }
    catch (...)
    {

        if (pthread_mutex_unlock(m_pMutexForWritingTable) != 0)
            return 0;
        std::cout << "Normal Unlock mutex" << std::endl;
    }
    return res;
}

int64_t **CLTable::SearchWithIndex(int attribute, int64_t low, int64_t high)
{
    pthread_mutex_lock(m_pMutexForWritingTable);
    std::cout << "Index Search locked!" << std::endl;
    int64_t **res = (int64_t **)malloc(sizeof(int64_t *) * 10);
    for (int i = 0; i < 10; i++)
    {
        res[i] = nullptr;
    }
    int cnt = 0;
    try
    {
        std::map<int64_t, int64_t>::iterator itfrom = RowsHash.lower_bound(low);
        // std::cout << itfrom->second <<std::endl;
        std::map<int64_t, int64_t>::iterator itto = RowsHash.upper_bound(high);
        // std::cout << itto->second << std::endl;
        for (std::map<int64_t, int64_t>::iterator it = itfrom; it != itto; ++it)
        {
            if(cnt ==0){
                std::cout << "Index search success!" << std::endl;
            }
            int64_t *tmp = m_pReader + it->second * 100;
            res[cnt] = (int64_t *)malloc(ROW_LENGTH); //输出范围内的有数据
            memcpy(res[cnt], tmp, ROW_LENGTH);
            cnt++;
            // std::cout << "cnt=" << cnt << std::endl;
            if (cnt == 10)
                break;
        }
        throw 0;
    }
    catch (...)
    {
        if (pthread_mutex_unlock(m_pMutexForWritingTable) != 0)
            return 0;
        std::cout <<"cnt = "<<cnt<<std::endl<< "Index Unlock mutex" << std::endl;
    }
    return res;
}

int CLTable::Flush()
{
    if (pthread_mutex_lock(m_pMutexForWritingTable) != 0)
        return -1;
    // pthread_mutex_lock(m_pMutexForWritingTable);
    try
    {
        if (m_nUsedRowsForBuffer == 0)
            throw 0;
        lseek(m_Fd, 0, SEEK_END);
        if (write(m_Fd, m_pTableBuffer, ROW_LENGTH * m_nUsedRowsForBuffer) == -1)
            throw -1;
        m_nRows += m_nUsedRowsForBuffer;
        m_nUsedRowsForBuffer = 0;
        // std::cout<< "Flush success"<<std::endl;
        throw 0;
    }
    catch (int &s)
    {
        if (pthread_mutex_unlock(m_pMutexForWritingTable) != 0)
            return -1;
        return s;
    }
}

int CLTable::WriteRow(const int64_t *pstrMsg)
{
    if (pstrMsg == 0)
        return -1;
    if (m_nUsedRowsForBuffer < BUFFER_ROW)
    {
        pthread_mutex_lock(m_pMutexForWritingTable);
        try
        {
            memcpy(m_pTableBuffer + (100 * m_nUsedRowsForBuffer), pstrMsg, ROW_LENGTH);
            // std::cout << m_pTableBuffer << "     " << m_pTableBuffer+(100 * m_nUsedRowsForBuffer) << std::endl;
            m_nUsedRowsForBuffer++;

            throw 0;
        }
        catch (int &s)
        {
            if (pthread_mutex_unlock(m_pMutexForWritingTable) != 0)
                return -1;
            CLTable::Flush();
            return 0;
        }
    }
    return -1;
}

int CLTable::ReadRow()
{
    if (m_nReaderRows == m_nRows)
    {
        std::cout << "m_nReaderRows = " << m_nReaderRows << "     "
                  << "m_nRows" << m_nRows << std::endl;
        return 0;
    }
    if (m_nRows == 0)
        return 0;

    pthread_mutex_lock(m_pMutexForWritingTable);
    // std::cout << "m_pMutexForWritingTable locked!" << std::endl;
    if (m_nReaderRows == m_nRows)
    {
        if (pthread_mutex_unlock(m_pMutexForWritingTable) != 0)
            return -1;
        // std::cout << "m_pMutexForWritingTable unlocked!" << std::endl;
        return 0;
    }
    try
    {

        // std::cout << "m_pReader free!" << std::endl;
        // m_nReaderRows = m_nRows;
        m_pReader = (int64_t *)realloc(m_pReader, ROW_LENGTH * m_nRows);
        // std::cout << "m_pReader malloc again!" << std::endl;
        lseek(m_Fd, ROW_LENGTH * m_nReaderRows, SEEK_SET);
        read(m_Fd, m_pReader + 100 * m_nReaderRows, ROW_LENGTH * (m_nRows - m_nReaderRows));
        // std::cout << "m_pReader Refresh!" << std::endl;
        int num = m_nRows - m_nReaderRows;
        throw num;
    }
    catch (int &s)
    {
        m_nReaderRows = m_nRows;
        if (pthread_mutex_unlock(m_pMutexForWritingTable) != 0)
            return -1;
        // std::cout << "m_pMutexForWritingTable unlocked!" << std::endl;
        return s;
    }
}

CLTable *CLTable::GetInstance()
{
    if (m_pTable != 0){
        // std::cout<<"test"<<std::endl;
        return m_pTable;
    }
        
    // if (m_pMutexForCreatator == 0)
    //     return 0;


    pthread_mutex_lock(m_pMutexForCreatator);
    std::cout << "m_pMutexForCreatator locked!" <<std::endl;

    if (m_pTable == 0)
    {
        try
        {
            m_pTable = new CLTable();
        }
        catch (const char *)
        {
            pthread_mutex_unlock(m_pMutexForCreatator);
            std::cout<<"unlocked and 0"<<std::endl;
            return 0;
        }

        if (atexit(CLTable::OnProcessExit) != 0)
        {
            m_pTable->m_bFlagForProcessExit = true;

            if (pthread_mutex_unlock(m_pMutexForCreatator) != 0){
                std::cout << "unlocked and 1" << std::endl;
                return 0;
            }
        }
        else
        {
            pthread_mutex_unlock(m_pMutexForCreatator);
            std::cout << "unlocked and 2" << std::endl;
        }
        return m_pTable;
    }
    pthread_mutex_unlock(m_pMutexForCreatator);
    std::cout << "m_pMutexForCreatator unlocked!" << std::endl;
    return m_pTable;
}

void CLTable::OnProcessExit()
{
    CLTable *pTable = CLTable::GetInstance();
    if (pTable != 0)
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
        std::cout << "Before close the table, there are " << pTable->m_nRows << " rows in total." << std::endl;
        std::cout << "Before close the table, there are " << lseek(pTable->m_Fd, 0, SEEK_END) / ROW_LENGTH << " rows in file." << std::endl;
    }
    delete pTable->m_pMutexForWritingTable;
    delete[] pTable->m_pTableBuffer;
    close(pTable->m_Fd);
    std::cout << "OnProcessExit() has been execute!" << std::endl;
}

int CLTable::SaveIndex(int attribute)
{
    std::string s = "Index" + std::to_string(attribute);
    int fd = open(s.data(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if(lseek(fd,0,SEEK_END)/sizeof(int64_t) == m_nReaderRows*ROW_LENGTH){
        close(fd);
        return 0;
    }
    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * 2 * m_nReaderRows);
    int cnt = 0;
    for (int64_t i = 0; i < m_nReaderRows; i++)
    {
        RowsHash.insert(std::pair<int64_t, int64_t>(*(m_pReader + 100 * i + attribute), i));
        tmp[cnt++] = *(m_pReader + 100 * i + attribute);
        tmp[cnt++] = i;
    }
    lseek(fd, 0, SEEK_SET);
    write(fd, tmp, sizeof(int64_t) * 2 * m_nReaderRows);
    free(tmp);
    close(fd);
    // std::cout << "The map of attribute " << attribute << " has been built, and the size is " << RowsHash.size() << std::endl;

    return 0;
}

int CLTable::ReadIndex(int attribute)
{
    std::string s = "Index" + std::to_string(attribute);
    if ((access(s.data(), F_OK)) == -1)
    {
        std::cout << "IndexFile is not exist!" << std::endl;
        return 0;
    }
    int fd = open(s.data(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    int size = lseek(fd, 0, SEEK_END);
    int64_t *tmp = (int64_t *)malloc(size);
    lseek(fd, 0, SEEK_SET);
    read(fd, tmp, size);
    for (int64_t i = 0; i < (size / sizeof(int64_t)); i++)
    {
        // std::cout<<tmp[i]<<"      "<<tmp[i+1]<<std::endl;
        RowsHash.insert(std::pair<int64_t, int64_t>(tmp[i], tmp[++i]));
        // std::cout << RowsHash[i] << std::endl;
    }
    close(fd);
    free(tmp);
    return 1;
}


//多线程函数，随机完成多种任务
//insert -> search -> saveindex 
void * InsertSearchIndex(void* args){
    std::default_random_engine generator(time(NULL));
    std::uniform_int_distribution<int> dis(0, 12);
    auto dice = std::bind(dis, generator);


    CLTable *pTable = CLTable::GetInstance();

    for (int64_t i = 0; i <10; i++)
    {
        int64_t tmp[100];
        for (int i = 0; i < 100; i++)
        {
            tmp[i] = abs(dice());
        }
        std::cout << "Writing table"<< std::endl;
        pTable->WriteRowMsg(tmp);
    }
    for(int i= 0;i<10;i++){
        int64_t a = abs(dice());
        std::cout << "a = " << a << std::endl;
        std::cout << "Searching table" << std::endl;
        int64_t **res = pTable->SearchFromTable(67, 1, a);
        for (int i = 0; i < 10; i++)
        {
            if (res == 0 || res[i] == 0)
            {
                std::cout << "There are(is) " << i << " searched data in table!" << std::endl;
                break;
            }
            std::cout << "res[" << i << "][0] =  " << res[i][0]  << "      "
                      << "res[" << i << "][0] =  " << res[i][50] << "      "
                      << "res[" << i << "][0] =  " << res[i][99] << std::endl;
        }
    }
    std::cout << "Saving Index" << std::endl;
    pTable->SaveIndex(dice()+20);

}






int main()
{
    pthread_t tids[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; ++i)
    {
        //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
        int ret = pthread_create(&tids[i], NULL, InsertSearchIndex, NULL);
        if (ret != 0)
        {
            std::cout << "pthread_create error: error_code=" << ret << std::endl;
        }
        else{
            std::cout << "pthread has been created, and tid = " << tids[i] << std::endl;
        }
        sleep(3);
    }
    //等各个线程退出后，进程才结束，否则进程强制结束了，线程可能还没反应过来；
    pthread_exit(NULL);


    // std::default_random_engine generator(time(NULL));
    // std::uniform_int_distribution<int64_t> dis(0, 18446744073709551615);
    // auto dice = std::bind(dis, generator);
    // CLTable *pTable = CLTable::GetInstance();
    // for (int64_t i = 0; i < 10000; i++)
    // {
    //     int64_t tmp[100];
    //     for (int i = 0; i < 100; i++)
    //     {
    //         tmp[i] = abs(dice());
    //     }
    //     // std::cout << tmp[0] <<"    "<<tmp[99] << std::endl;
    //     pTable->WriteRowMsg(tmp);
    // }
    // int64_t a = abs(dice());
    // std::cout << "a = " << a << std::endl;
    // int64_t **res = pTable->SearchFromTable(50, 1, a + 1000000);
    // for (int i = 0; i < 10; i++)
    // {
    //     if (res == 0 || res[i] == 0)
    //     {
    //         std::cout << "There are(is) " << i << " searched data in table!" << std::endl;
    //         break;
    //     }
    //     std::cout << "res[" << i << "][0] =  " << res[i][0]  << "      "
    //               << "res[" << i << "][0] =  " << res[i][50] << "      "
    //               << "res[" << i << "][0] =  " << res[i][99] << std::endl;
    // }

    // pTable->SaveIndex(50);
    return 0;
}


