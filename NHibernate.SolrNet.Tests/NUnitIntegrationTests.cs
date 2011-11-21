#region license
// Copyright (c) 2007-2010 Mauricio Scheffer
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#endregion

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using log4net.Config;
using NUnit.Framework;
using Microsoft.Practices.ServiceLocation;
using NHibernate.SolrNet.Impl;
using NHibernate.Tool.hbm2ddl;
using NHibernate;
using NHibernate.Criterion;
using SolrNet;
using SolrNet.Impl;
using SolrNet.Impl.DocumentPropertyVisitors;
using SolrNet.Mapping;

namespace NHibernate.SolrNet.Tests {
    [TestFixture]
    [Category("Integration")]
    public class NUnitIntegrationTests {
        [Test]
        public void Insert() {
            using (var session = sessionFactory.OpenSession()) {
                session.Save(new Entity {
                    Id = "abcd",
                    Description = "Testing NH-Solr integration",
                    Tags = new[] {"cat1", "aoe"},
                });
                session.Flush();
            }
            using (var session = cfgHelper.OpenSession(sessionFactory)) {
                var entities = session.CreateSolrQuery("solr").List<Entity>();
                Assert.AreEqual(1, entities.Count);
                Assert.AreEqual(2, entities[0].Tags.Count);
            }
        }

        [Test]
        public void DoesntLeakMem() {
            using (var session = cfgHelper.OpenSession(sessionFactory)) {
                session.FlushMode = FlushMode.Never;
                session.Save(new Entity {
                    Id = "abcd",
                    Description = "Testing NH-Solr integration",
                    Tags = new[] { "cat1", "aoe" },
                });
            }
            var listener = cfg.EventListeners.PostInsertEventListeners[0];
            var addField = typeof (SolrNetListener<Entity>).GetField("entitiesToAdd", BindingFlags.NonPublic | BindingFlags.Instance);
            var addDict = (IDictionary<ITransaction, List<Entity>>)addField.GetValue(listener);
            Assert.AreEqual(0, addDict.Count);
        }

        private const int _threadsNumber = 10;
        private static ManualResetEvent[] _resetEvents;
        private static int _errorsCount = 0;
        private static int _requestsCount = 0;
        [Test]
        public void Should_Insert_200_Rows_For_10_ConcurrentUsers()
        {
            _resetEvents = new ManualResetEvent[_threadsNumber];
            for (int s = 0; s < _threadsNumber; s++)
            {
                _resetEvents[s] = new ManualResetEvent(false);
                ThreadPool.QueueUserWorkItem(new WaitCallback(DoWork), (object)s);
            }

            //wait for all threads to complete
            System.Diagnostics.Debug.WriteLine(string.Format("All threads started: {0}", DateTime.Now));
            WaitHandle.WaitAll(_resetEvents);
            System.Diagnostics.Debug.WriteLine(string.Format("All threads finished: {0}", DateTime.Now));

            Assert.AreEqual(200, _requestsCount);

            using (var session = cfgHelper.OpenSession(sessionFactory))
            {
                var entities = session.QueryOver<Entity>().List();
                Assert.AreEqual(200, entities.Count);
                //Gallio.Framework.DiagnosticLog.WriteLine(entities.Count.ToString());
                //System.Diagnostics.Debug.WriteLine(entities.Count.ToString());
            }

            using (var session = cfgHelper.OpenSession(sessionFactory))
            {
                var entities = session.CreateSolrQuery("nhibernate").List<Entity>();
                //Gallio.Framework.DiagnosticLog.WriteLine(entities.Count.ToString());
                //System.Diagnostics.Debug.WriteLine(entities.Count.ToString());
                Assert.AreEqual(200, entities.Count);
            }

            System.Diagnostics.Debug.WriteLine(string.Format("Solr errors: {0}", _errorsCount));
        }

        private void DoWork(object o)
        {
            int index = (int)o;
            try
            {
                for (int i = 0; i < 20; i++)
                {
                    try
                    {
                        Interlocked.Increment(ref _requestsCount);

                        //System.Diagnostics.Debug.WriteLine(string.Format("[Thread {0}:{1}] {2}", index, i, "OK"));
                        using (var session = sessionFactory.OpenSession())
                        {
                            session.Save(new Entity
                            {
                                Id = string.Format("{0}#{1}", index, i),
                                Description = "NHibernate integration",
                                Tags = new[] { "cat1", "aoe" },
                            });
                            session.Flush();
                        }
                    }
                    catch(Exception ex)
                    {
                        //Gallio.Framework.DiagnosticLog.WriteLine(ex);
                        System.Diagnostics.Debug.WriteLine(string.Format("[Thread {0}:{1}] {2}", index, i, "Error"));
                        //System.Diagnostics.Debug.WriteLine(string.Format("[Thread {0}:{1}] {2}", index, i, "Error"));
                        Interlocked.Increment(ref _errorsCount);
                    }

                }
            }
            catch
            {
                Interlocked.Increment(ref _errorsCount);
            }
            finally
            {
                _resetEvents[index].Set();
            }
        }


        private Configuration SetupNHibernate() {
            var cfg = ConfigurationExtensions.GetEmptyNHConfigForSqlExpress();
            cfg.AddXmlString(@"<?xml version='1.0' encoding='utf-8' ?>
<hibernate-mapping xmlns='urn:nhibernate-mapping-2.2' default-lazy='false'>
  <class name='NHibernate.SolrNet.Tests.Entity, NHibernate.SolrNet.Tests'>
    <id name='Id'>
      <generator class='assigned'/>
    </id>
    <property name='Description'/>
  </class>
</hibernate-mapping>");
            new SchemaExport(cfg).Execute(false, true, false);
            return cfg;
        }

        private void SetupSolr() {
            Startup.InitContainer();

            Startup.Container.Remove<IReadOnlyMappingManager>();
            var mapper = new MappingManager();
            mapper.Add(typeof (Entity).GetProperty("Description"), "name");
            mapper.Add(typeof (Entity).GetProperty("Id"), "id");
            mapper.Add(typeof (Entity).GetProperty("Tags"), "cat");
            Startup.Container.Register<IReadOnlyMappingManager>(c => mapper);

            Startup.Container.Remove<ISolrDocumentPropertyVisitor>();
            var propertyVisitor = new DefaultDocumentVisitor(mapper, Startup.Container.GetInstance<ISolrFieldParser>());
            Startup.Container.Register<ISolrDocumentPropertyVisitor>(c => propertyVisitor);

            Startup.Init<Entity>("http://localhost:8983/solr");
            var solr = ServiceLocator.Current.GetInstance<ISolrOperations<Entity>>();
            solr.Delete(SolrQuery.All);
            solr.Commit();
        }

        [TestFixtureSetUp]
        public void FixtureSetup() {
            BasicConfigurator.Configure();
            SetupSolr();

            cfg = SetupNHibernate();

            cfgHelper = new CfgHelper();
            cfgHelper.Configure(cfg, true);
            sessionFactory = cfg.BuildSessionFactory();
        }

        [TestFixtureTearDown]
        public void FixtureTearDown() {
            sessionFactory.Dispose();
        }

        private Configuration cfg;
        private CfgHelper cfgHelper;
        private ISessionFactory sessionFactory;
    }
}