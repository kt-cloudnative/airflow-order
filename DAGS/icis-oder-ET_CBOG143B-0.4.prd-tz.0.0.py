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
                , WORKFLOW_NAME='ET_CBOG143B',WORKFLOW_ID='1bd4a668a9714253a6be0d2834084f6c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG143B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 3, 16, 2, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1bd4a668a9714253a6be0d2834084f6c')

    cbog143bJob_vol = []
    cbog143bJob_volMnt = []
    cbog143bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog143bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog143bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog143bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog143bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog143bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7dd9408a1de44b2fa81ef7192310115d',
        'volumes': cbog143bJob_vol,
        'volume_mounts': cbog143bJob_volMnt,
        'env_from':cbog143bJob_env,
        'task_id':'cbog143bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.6',
        'arguments':["--job.name=cbog143bJob", "inputFile=kt_initbill_othr_202412.dat", "inProcGb=1", "inDelFlg=N", "regEmpNo=91108904", "regOfcCd=700141", "regEmpName=cbog143b", "test=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1bd4a668a9714253a6be0d2834084f6c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog143bJob,
        Complete
    ]) 

    # authCheck >> cbog143bJob >> Complete
    workflow








