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
                , WORKFLOW_NAME='IN_CBON323B',WORKFLOW_ID='817ae92fa8ae4f75b2b9e6917dfd1462', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBON323B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('817ae92fa8ae4f75b2b9e6917dfd1462')

    cbon323bJob_vol = []
    cbon323bJob_volMnt = []
    cbon323bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon323bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon323bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbon323bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon323bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon323bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5aaa39acf4434c948d454b3d48207641',
        'volumes': cbon323bJob_vol,
        'volume_mounts': cbon323bJob_volMnt,
        'env_from':cbon323bJob_env,
        'task_id':'cbon323bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.2',
        'arguments':["--job.name=cbon323bJob", "endTranDate=20240816", "date=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('817ae92fa8ae4f75b2b9e6917dfd1462')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon323bJob,
        Complete
    ]) 

    # authCheck >> cbon323bJob >> Complete
    workflow








