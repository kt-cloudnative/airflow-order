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
                , WORKFLOW_NAME='WC_CBOL207B',WORKFLOW_ID='25c257641d0b4f60b72b1710f9bd3c2a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOL207B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 31, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('25c257641d0b4f60b72b1710f9bd3c2a')

    cbol207bJob_vol = []
    cbol207bJob_volMnt = []
    cbol207bJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    cbol207bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbol207bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol207bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '846057b14eca47448dfc16799ae2b9a5',
        'volumes': cbol207bJob_vol,
        'volume_mounts': cbol207bJob_volMnt,
        'env_from':cbol207bJob_env,
        'task_id':'cbol207bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.2',
        'arguments':["--job.name=cbol207bJob", "endTranDate=20240731", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('25c257641d0b4f60b72b1710f9bd3c2a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol207bJob,
        Complete
    ]) 

    # authCheck >> cbol207bJob >> Complete
    workflow








