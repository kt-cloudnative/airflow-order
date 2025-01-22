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
                , WORKFLOW_NAME='PP_CBON311B',WORKFLOW_ID='4fdf157a74464998b8e065b626e823e9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON311B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 6, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4fdf157a74464998b8e065b626e823e9')

    cbon311bJob_vol = []
    cbon311bJob_volMnt = []
    cbon311bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon311bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon311bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon311bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '49e217b291584e7f835758b182a24b9c',
        'volumes': cbon311bJob_vol,
        'volume_mounts': cbon311bJob_volMnt,
        'env_from':cbon311bJob_env,
        'task_id':'cbon311bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.55',
        'arguments':["--job.name=cbon311bJob", "endTranDate=20240731", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4fdf157a74464998b8e065b626e823e9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon311bJob,
        Complete
    ]) 

    # authCheck >> cbon311bJob >> Complete
    workflow








