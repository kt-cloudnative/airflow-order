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
                , WORKFLOW_NAME='EI_CBOT390B',WORKFLOW_ID='755cf6330126476381baf216c8ea9718', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CBOT390B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('755cf6330126476381baf216c8ea9718')

    cbot390bJob_vol = []
    cbot390bJob_volMnt = []
    cbot390bJob_env = [getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISConfigMap('icis-oder-entprinet-batch-configmap2'), getICISSecret('icis-oder-entprinet-batch-secret')]
    cbot390bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot390bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot390bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7b01545f647c4f018ce995dbb14b5bca',
        'volumes': cbot390bJob_vol,
        'volume_mounts': cbot390bJob_volMnt,
        'env_from':cbot390bJob_env,
        'task_id':'cbot390bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.14',
        'arguments':["--job.name=cbot390bJob", "requestDate="+str(datetime.now()), "procDate="+str(datetime.now().strftime("%Y%m%d")), "progName=cbot390b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbot405bJob_vol = []
    cbot405bJob_volMnt = []
    cbot405bJob_env = [getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISConfigMap('icis-oder-entprinet-batch-configmap2'), getICISSecret('icis-oder-entprinet-batch-secret')]
    cbot405bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot405bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot405bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c8349753143c4f83bc7f2b036405dd28',
        'volumes': cbot405bJob_vol,
        'volume_mounts': cbot405bJob_volMnt,
        'env_from':cbot405bJob_env,
        'task_id':'cbot405bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.14',
        'arguments':["--job.name=cbot405bJob", "requestDate="+str(datetime.now()), "procDate="+str(datetime.now().strftime("%Y%m%d")), "progName=cbot405b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('755cf6330126476381baf216c8ea9718')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot405bJob,
        cbot390bJob,
        Complete
    ]) 

    # authCheck >> cbot405bJob >> cbot390bJob >> Complete
    workflow








