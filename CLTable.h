#pragma once
#include <pthread.h>

class CLTable
{
public:
    static CLTable *GetInstance();
    static int WriteRowMsg(const int64_t *pstrMsg);
    static int ReadRowMsg();
    int64_t *m_pReader;
    int WriteRow(const int64_t *pstrMsg);
    int ReadRow();
    int Flush();

private:
    static void OnProcessExit();

    static pthread_mutex_t *InitializeMutex();

private:
    CLTable(const CLTable&);
    CLTable& operator=(const CLTable&);

    CLTable();
    ~CLTable();

private:
    int m_Fd;
    int64_t m_nRows;
    int64_t m_nReaderRows;
    
    pthread_mutex_t *m_pMutexForWritingTable;
    pthread_mutex_t *m_pMutexForReadingTable;
    int m_nReaders;

    static CLTable *m_pTable;
    static pthread_mutex_t *m_pMutexForCreatator;

private:
    int64_t *m_pTableBuffer;
    int64_t m_nUsedRowsForBuffer;

private:
    bool m_bFlagForProcessExit;

};