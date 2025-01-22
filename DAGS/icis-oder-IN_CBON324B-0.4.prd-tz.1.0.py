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
                , WORKFLOW_NAME='IN_CBON324B',WORKFLOW_ID='9a95e1e961c446c0beec695ef29f8195', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBON324B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9a95e1e961c446c0beec695ef29f8195')

    cbon324bJob_vol = []
    cbon324bJob_volMnt = []
    cbon324bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon324bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon324bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbon324bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon324bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon324bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f6de1dcf0a754676b10f6f7209bd449d',
        'volumes': cbon324bJob_vol,
        'volume_mounts': cbon324bJob_volMnt,
        'env_from':cbon324bJob_env,
        'task_id':'cbon324bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.2',
        'arguments':["--job.name=cbon324bJob", "endTranDate=20240816", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9a95e1e961c446c0beec695ef29f8195')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon324bJob,
        Complete
    ]) 

    # authCheck >> cbon324bJob >> Complete
    workflow








