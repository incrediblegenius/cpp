#pragma once
#include <pthread.h>

class CLTable
{
public:
    static CLTable *GetInstance();
    static int WriteRowMsg(const int64_t *pstrMsg);
    static int ReadRowMsg();
    // int64_t *m_pReader;
    // int64_t m_nReaderRows;
    static int64_t **SearchFromTable(int attribute, int64_t low, int64_t high);

private:
    static void OnProcessExit();
    int64_t *m_pReader;
    static pthread_mutex_t *InitializeMutex();

private:
    CLTable(const CLTable&);
    CLTable& operator=(const CLTable&);
 
    int WriteRow(const int64_t *pstrMsg);
    int ReadRow();
    int Flush();
    int64_t** Search(int attribute, int64_t low, int64_t high);

    CLTable();
    ~CLTable();

private:
    int m_Fd;
    int64_t m_nRows;
    int64_t m_nReaderRows;
    
    pthread_mutex_t *m_pMutexForWritingTable;
    // int m_nReaders;

    static CLTable *m_pTable;
    static pthread_mutex_t *m_pMutexForCreatator;

private:
    int64_t *m_pTableBuffer;
    int64_t m_nUsedRowsForBuffer;

private:
    bool m_bFlagForProcessExit;

};
