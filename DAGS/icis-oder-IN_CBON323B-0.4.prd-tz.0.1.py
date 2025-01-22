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
                , WORKFLOW_NAME='IN_CBON323B',WORKFLOW_ID='598995495fa34e2389da37a76149faca', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBON323B-0.4.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('598995495fa34e2389da37a76149faca')

    cbon323bJob_vol = []
    cbon323bJob_volMnt = []
    cbon323bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon323bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon323bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbon323bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon323bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon323bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '169a55d01f3244e1b43af7c256cce4ca',
        'volumes': cbon323bJob_vol,
        'volume_mounts': cbon323bJob_volMnt,
        'env_from':cbon323bJob_env,
        'task_id':'cbon323bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cbon323bJob", "endTranDate=20240731", "date=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('598995495fa34e2389da37a76149faca')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon323bJob,
        Complete
    ]) 

    # authCheck >> cbon323bJob >> Complete
    workflow








