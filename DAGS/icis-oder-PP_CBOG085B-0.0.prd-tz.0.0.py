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
                , WORKFLOW_NAME='PP_CBOG085B',WORKFLOW_ID='2f911821008d436d8f5b3cd8ddc6d9b1', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG085B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2f911821008d436d8f5b3cd8ddc6d9b1')

    cbog085bJob_vol = []
    cbog085bJob_volMnt = []
    cbog085bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog085bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog085bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog085bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog085bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog085bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b7f51a448bbe45558826f476ad2e5ee4',
        'volumes': cbog085bJob_vol,
        'volume_mounts': cbog085bJob_volMnt,
        'env_from':cbog085bJob_env,
        'task_id':'cbog085bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.74',
        'arguments':["--job.name=cbog085bJob", "inputFileName=sin191225s99", "regOfcCd=R00422","regEmpNo=91353523","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2f911821008d436d8f5b3cd8ddc6d9b1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog085bJob,
        Complete
    ]) 

    # authCheck >> cbog085bJob >> Complete
    workflow








