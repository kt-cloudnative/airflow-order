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
                , WORKFLOW_NAME='PP_CSNG917B',WORKFLOW_ID='3e4d9a67719f4b8fb9af69c1166848eb', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG917B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 16, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3e4d9a67719f4b8fb9af69c1166848eb')

    csng917bJob_vol = []
    csng917bJob_volMnt = []
    csng917bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng917bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng917bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng917bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bfb3038d8c5d4a97ac08eedd8b4f1c2e',
        'volumes': csng917bJob_vol,
        'volume_mounts': csng917bJob_volMnt,
        'env_from':csng917bJob_env,
        'task_id':'csng917bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.50',
        'arguments':["--job.name=csng917bJob", "inMonth=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3e4d9a67719f4b8fb9af69c1166848eb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng917bJob,
        Complete
    ]) 

    # authCheck >> csng917bJob >> Complete
    workflow








