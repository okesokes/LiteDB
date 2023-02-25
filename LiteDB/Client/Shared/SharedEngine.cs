using LiteDB.Engine;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
#if NETFRAMEWORK
using System.Security.AccessControl;
using System.Security.Principal;
#endif

namespace LiteDB
{
    public class SharedEngine : ILiteEngine
    {
        private readonly EngineSettings _settings;
        private readonly Mutex _mutex;
        private LiteEngine _engine;
        private Random rand = new Random(
                    Guid.NewGuid().GetHashCode()
                    );

        //Thread safe bool, default is 0 as false;
        private volatile int _threadSafeBoolBackValueForIsMutexLocking = 0;
        private bool isMutexLocking
        {
            get
            {
                return (Interlocked.CompareExchange(ref _threadSafeBoolBackValueForIsMutexLocking, 1, 1) == 1);
            }
            set
            {
                if (value) Interlocked.CompareExchange(ref _threadSafeBoolBackValueForIsMutexLocking, 1, 0);
                else Interlocked.CompareExchange(ref _threadSafeBoolBackValueForIsMutexLocking, 0, 1);
            }
        }

        //Thread safe bool, default is 0 as false;
        private volatile int _threadSafeBoolBackValueForIsDisposed = 0;
        private bool isDisposed
        {
            get
            {
                return (Interlocked.CompareExchange(ref _threadSafeBoolBackValueForIsDisposed, 1, 1) == 1);
            }
            set
            {
                if (value) Interlocked.CompareExchange(ref _threadSafeBoolBackValueForIsDisposed, 1, 0);
                else Interlocked.CompareExchange(ref _threadSafeBoolBackValueForIsDisposed, 0, 1);
            }
        }


        public SharedEngine(EngineSettings settings)
        {
            _settings = settings;

            var name = Path.GetFullPath(settings.Filename).ToLower().Sha1();

            try
            {
#if NETFRAMEWORK
                var allowEveryoneRule = new MutexAccessRule(
                    new SecurityIdentifier(WellKnownSidType.WorldSid, null),
                    MutexRights.FullControl, AccessControlType.Allow);

                var securitySettings = new MutexSecurity();
                securitySettings.AddAccessRule(allowEveryoneRule);

                _mutex = new Mutex(false, "Global\\" + name + ".Mutex", out _, securitySettings);
#else
                _mutex = new Mutex(false, "Global\\" + name + ".Mutex");
#endif
            }
            catch (NotSupportedException ex)
            {
                throw new PlatformNotSupportedException
                    ("Shared mode is not supported in platforms that do not implement named mutex.", ex);
            }
        }

        /// <summary>
        /// Open database in safe mode
        /// </summary>
        private void OpenDatabase()
        {
            if (!isMutexLocking)
            {
                lock (_mutex)
                {
                    isMutexLocking = true;
                    if (_engine == null)
                    {
                        try
                        {
                            _mutex.WaitOne();
                        }
                        catch (AbandonedMutexException) { }
                        catch { isMutexLocking = false; throw; }

                        try
                        {
                            _engine = new LiteEngine(_settings);
                            isDisposed = false;
                        }
                        catch
                        {
                            try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                            isMutexLocking = false;
                            throw;
                        }
                    }
                    isMutexLocking = false;
                }
            }
            else
            {
                if (_engine == null)
                {
                    try
                    {
                        _engine = new LiteEngine(_settings);
                        isDisposed = false;
                    }
                    catch
                    {
                        try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                        isMutexLocking = false;
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Dequeue stack and dispose database on empty stack
        /// </summary>
        private void CloseDatabase()
        {
            if (!isMutexLocking)
            {
                lock (_mutex)
                {
                    isMutexLocking = true;
                    if (_engine != null)
                    {
                        this.Dispose(true);
                    }
                    isMutexLocking = false;
                }
            }
            else
            {
                if (_engine != null)
                {
                    this.Dispose(true);
                }
            }
        }

        #region Transaction Operations

        public bool BeginTrans()
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;

                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                bool val = false;
                try
                {
                    var result = _engine.BeginTrans();
                    /*
                    if(result == false)
                    {

                    }
                    */
                    val = result;
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public bool Commit()
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;

                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                if (_engine == null || isDisposed)
                {
                    isDisposed = true;
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    return false;
                }

                bool val = false;
                try
                {
                    val = _engine.Commit();
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public bool Rollback()
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;

                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                if (_engine == null || isDisposed)
                {
                    isDisposed = true;
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    return false;
                }

                bool val = false;
                try
                {
                    val = _engine.Rollback();
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        #endregion

        #region Read Operation

        public IBsonDataReader Query(string collection, Query query)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;

                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                SharedDataReader bsonDataReader = null;
                try
                {
                    var reader = _engine.Query(collection, query);
                    bsonDataReader = new SharedDataReader(reader, () => this.Dispose("Query"));
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                if (bsonDataReader != null)
                {
                    return bsonDataReader;
                }
                else
                {
                    throw new NullReferenceException("Nulled SharedDataReader.");
                }
            }
        }

        public BsonValue Pragma(string name)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;

                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                BsonValue val = null;
                try
                {
                    val = _engine.Pragma(name);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public bool Pragma(string name, BsonValue value)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                bool val = false;
                try
                {
                    val = _engine.Pragma(name, value);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        #endregion

        #region Write Operations

        public int Checkpoint()
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                int val;
                try
                {
                    val = _engine.Checkpoint();
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public long Rebuild(RebuildOptions options)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                long val;
                try
                {
                    val = _engine.Rebuild(options);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public int Insert(string collection, IEnumerable<BsonDocument> docs, BsonAutoId autoId)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                int val;
                try
                {
                    val = _engine.Insert(collection, docs, autoId);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public int Update(string collection, IEnumerable<BsonDocument> docs)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                int val;
                try
                {
                    val = _engine.Update(collection, docs);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public int UpdateMany(string collection, BsonExpression extend, BsonExpression predicate)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                int val;
                try
                {
                    val = _engine.UpdateMany(collection, extend, predicate);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();

                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public int Upsert(string collection, IEnumerable<BsonDocument> docs, BsonAutoId autoId)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                int val;
                try
                {
                    val = _engine.Upsert(collection, docs, autoId);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();
                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public int Delete(string collection, IEnumerable<BsonValue> ids)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                int val;
                try
                {
                    val = _engine.Delete(collection, ids);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();
                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public int DeleteMany(string collection, BsonExpression predicate)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                int val;
                try
                {
                    val = _engine.DeleteMany(collection, predicate);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();
                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public bool DropCollection(string name)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                bool val = false;
                try
                {
                    val = _engine.DropCollection(name);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();
                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public bool RenameCollection(string name, string newName)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                bool val = false;
                try
                {
                    val = _engine.RenameCollection(name, newName);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();
                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public bool DropIndex(string collection, string name)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                bool val = false;
                try
                {
                    val = _engine.DropIndex(collection, name);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();
                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }

        public bool EnsureIndex(string collection, string name, BsonExpression expression, bool unique)
        {
            EnsureLocking();
            lock (_mutex)
            {
                isMutexLocking = true;
                try
                {
                    _mutex.WaitOne();
                }
                catch (AbandonedMutexException) { }
                catch { isMutexLocking = false; throw; }

                this.OpenDatabase();

                bool val = false;
                try
                {
                    val = _engine.EnsureIndex(collection, name, expression, unique);
                }
                catch
                {
                    this.CloseDatabase();
                    try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                    isMutexLocking = false;
                    throw;
                }

                this.CloseDatabase();
                try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
                isMutexLocking = false;
                return val;
            }
        }
        #endregion

        private void EnsureLocking()
        {
            int count = 0;
            System.Threading.Tasks.Task delay;
            while (isMutexLocking)
            {
                delay = System.Threading.Tasks.Task.Delay(
                    TimeSpan.FromMilliseconds(
                        rand.Next(3, 7)
                        )
                    );
                delay.Wait();
                if (!isMutexLocking) { break; }
                if (delay.IsCompleted)
                {
                    ++count;

                    if (count > 100)
                    {
                        count = 0;
                        throw new TimeoutException("Shared threading controller with Mutex is Locking.");
                    }
                }
            }
        }

        public void Dispose()
        {
            this.Dispose(false);
        }

        public void Dispose(string fromQuery = "yes")
        {
            this.Dispose(true);
            isMutexLocking = false;
            GC.SuppressFinalize(this);
        }

        ~SharedEngine()
        {
            this.Dispose(false);
            try { _mutex.ReleaseMutex(); } catch { isMutexLocking = false; throw; }
            isMutexLocking = false;
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_engine == null || isDisposed)
            {
                isDisposed = true;
                return;
            }

            if (disposing)
            {
                if (_engine != null)
                {
                    if (!isMutexLocking)
                    {
                        lock (_mutex)
                        {
                            isMutexLocking = true;

                            try
                            {
                                _engine.Dispose();
                                _engine = null;
                            }
                            catch
                            {
                                _engine = null;
                            }
                            finally
                            {
                                isDisposed = true;
                            }

                            isMutexLocking = false;
                        }
                    }
                    else
                    {
                        try
                        {
                            _engine.Dispose();
                            _engine = null;
                        }
                        catch
                        {
                            _engine = null;
                        }
                        finally
                        {
                            isDisposed = true;
                        }
                    }
                }
            }

            GC.Collect();
        }
    }
}