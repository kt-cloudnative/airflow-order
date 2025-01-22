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
                , WORKFLOW_NAME='PP_CSNG633B',WORKFLOW_ID='1e2c142160274e1ea98aa9f2710d76c9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG633B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 14, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1e2c142160274e1ea98aa9f2710d76c9')

    csng633bJob_vol = []
    csng633bJob_volMnt = []
    csng633bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng633bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng633bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng633bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng633bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng633bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng633bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng633bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9c6f60b7ac0840eab61bad7e3d2a06c1',
        'volumes': csng633bJob_vol,
        'volume_mounts': csng633bJob_volMnt,
        'env_from':csng633bJob_env,
        'task_id':'csng633bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.50',
        'arguments':["--job.name=csng633bJob", "requestDate="+str(datetime.now().strftime("${YYYYMMDDHHMISSSSS}"))
, "pgmNm=csng633b" ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1e2c142160274e1ea98aa9f2710d76c9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng633bJob,
        Complete
    ]) 

    # authCheck >> csng633bJob >> Complete
    workflow








