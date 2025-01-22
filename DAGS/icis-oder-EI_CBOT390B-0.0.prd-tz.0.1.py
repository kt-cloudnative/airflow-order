from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='EI_CBOT390B',WORKFLOW_ID='e73b9431dc604ceb89363c35632dfa8c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CBOT390B-0.0.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 15, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e73b9431dc604ceb89363c35632dfa8c')

    cbot390bJob_vol = []
    cbot390bJob_volMnt = []
    cbot390bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot390bJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    cbot390bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot390bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c8476b3141a24c73b736d5203644a4cd',
        'volumes': cbot390bJob_vol,
        'volume_mounts': cbot390bJob_volMnt,
        'env_from':cbot390bJob_env,
        'task_id':'cbot390bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.14',
        'arguments':["--job.name=cbot390bJob", "requestDate="+str(datetime.now()), "procDate=20241129", "progName=cbot390b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbot405bJob_vol = []
    cbot405bJob_volMnt = []
    cbot405bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot405bJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    cbot405bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot405bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '73bad0e6de4a4051a634d33b24440dbf',
        'volumes': cbot405bJob_vol,
        'volume_mounts': cbot405bJob_volMnt,
        'env_from':cbot405bJob_env,
        'task_id':'cbot405bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.14',
        'arguments':["--job.name=cbot405bJob", "requestDate="+str(datetime.now()), "procDate=20241129", "progName=cbot405b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e73b9431dc604ceb89363c35632dfa8c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot405bJob,
        cbot390bJob,
        Complete
    ]) 

    # authCheck >> cbot405bJob >> cbot390bJob >> Complete
    workflow








