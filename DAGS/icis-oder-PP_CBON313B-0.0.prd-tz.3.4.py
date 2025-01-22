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
                , WORKFLOW_NAME='PP_CBON313B',WORKFLOW_ID='e838d0ba7be14ed7bee162d61388028f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON313B-0.0.prd-tz.3.4'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 22, 16, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e838d0ba7be14ed7bee162d61388028f')

    cbon313bJob_vol = []
    cbon313bJob_volMnt = []
    cbon313bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon313bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon313bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon313bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon313bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon313bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8ae43ddf733742688f23a113d285fe7c',
        'volumes': cbon313bJob_vol,
        'volume_mounts': cbon313bJob_volMnt,
        'env_from':cbon313bJob_env,
        'task_id':'cbon313bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbon313bJob","endTranDate=20240530","requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e838d0ba7be14ed7bee162d61388028f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon313bJob,
        Complete
    ]) 

    # authCheck >> cbon313bJob >> Complete
    workflow








