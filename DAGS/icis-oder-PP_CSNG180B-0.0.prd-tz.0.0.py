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
                , WORKFLOW_NAME='PP_CSNG180B',WORKFLOW_ID='2fdb0f5936584916a2ae515e5d638f45', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG180B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 18, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2fdb0f5936584916a2ae515e5d638f45')

    csng180bJob_vol = []
    csng180bJob_volMnt = []
    csng180bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng180bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng180bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng180bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng180bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng180bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4b24c40535a64718afa2bf311f0bedf6',
        'volumes': csng180bJob_vol,
        'volume_mounts': csng180bJob_volMnt,
        'env_from':csng180bJob_env,
        'task_id':'csng180bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng180bJob" ,"workDate=20250103", "workRsCd=1" , "workType=S" ,"empNo=91354703" , "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2fdb0f5936584916a2ae515e5d638f45')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng180bJob,
        Complete
    ]) 

    # authCheck >> csng180bJob >> Complete
    workflow








